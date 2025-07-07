from fastapi import FastAPI, Request, Form, Depends, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
import os
print("Current directory:", os.getcwd())
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from cachetools import TTLCache
import json
import logging
import uvicorn
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import asyncio
# this is the push for new branch updates
# Kubernetes imports
from kubernetes import client, config, watch, stream
from kubernetes.client.rest import ApiException
from fastapi import FastAPI, Request
from api_logger import log_request


# Your existing data fetcher functions
from aws_data_fetcher import (
    get_live_eks_data,
    get_single_cluster_details,
    upgrade_nodegroup_version,
    get_cluster_metrics,
    get_k8s_api_client
)

load_dotenv()
print("AWS_REGIONS =", os.getenv("AWS_REGIONS"))
print("AWS_TARGET_ACCOUNTS_ROLES =", os.getenv("AWS_TARGET_ACCOUNTS_ROLES"))
app = FastAPI(title="EKS Operational Dashboard")

# --- Middleware ---
@app.middleware("http")
async def log_incoming_requests(request: Request, call_next):
    method = request.method
    url = str(request.url)
    ip = request.client.host

    # Get body size if applicable
    if method in ("POST", "PUT", "PATCH"):
        body = await request.body()
        sent_size = len(body)
        request._body = body  # reinject body for downstream handlers
    elif method == "GET":
        sent_size = len(request.url.query.encode())
    else:
        sent_size = 0

    # Get response and measure response size
    response = await call_next(request)
    response_body = b""
    async for chunk in response.body_iterator:
        response_body += chunk
    response_size = len(response_body)

    # Create a new response to return (since body_iterator is exhausted)
    final_response = Response(
        content=response_body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type
    )

    # Log it
    log_request(response_size)

    return final_response



class UserStateMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request.state.user = request.session.get("user")
        response = await call_next(request)
        return response

app.add_middleware(UserStateMiddleware)
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY", "a_very_secret_key_for_dev"))
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")
cache = TTLCache(maxsize=200, ttl=3600)

# --- Auth Dependency ---
async def get_current_user(request: Request):
    return {"email": "test@example.com", "attributes": {}, "session_index": "test"}

# --- Role Resolver ---
def get_role_arn_for_account(account_id: str) -> str | None:
    target_roles_str = os.getenv("AWS_TARGET_ACCOUNTS_ROLES", "")
    for r_arn in target_roles_str.split(','):
        if f":{account_id}:" in r_arn:
            return r_arn.strip()
    return None

# --- Safe boto3 session creation ---
def get_session(role_arn: str):
    try:
        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="EKSAssumeRoleSession"
        )
        credentials = assumed_role_object["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"]
        )
    except NoCredentialsError:
        logging.error("No AWS credentials found. Please set them in env, AWS CLI, or IAM role.")
        raise
    except Exception as e:
        logging.error(f"Error assuming role: {e}")
        raise

# --- Dashboard Data ---
def get_dashboard_data(user: dict):
    saml_attributes = user.get("attributes", {})
    group_map_str = os.getenv("GROUP_TO_ACCOUNT_MAP", "")
    group_key = next((k for k in saml_attributes if 'Group' in k), None)
    saml_groups = saml_attributes.get(group_key, [])
    user_groups = saml_groups if isinstance(saml_groups, list) else [saml_groups]
    cache_key = f"dashboard_data_{'_'.join(sorted(user_groups))}"

    if cache_key in cache:
        logging.info(f"Cache HIT for dashboard data: user='{user.get('email')}'")
        return cache[cache_key]
    else:
        logging.info(f"Cache MISS for dashboard data: user='{user.get('email')}'")
        dashboard_data = get_live_eks_data()
        if not dashboard_data.get("errors"):
            cache[cache_key] = dashboard_data
        return dashboard_data

# --- Routes ---
@app.get("/", response_class=HTMLResponse)
async def read_dashboard(request: Request, user: dict = Depends(get_current_user)):
    request.state.now = datetime.now(timezone.utc)
    dashboard_data = get_dashboard_data(user)
    context = {"request": request, "user": user, **dashboard_data}
    return templates.TemplateResponse("dashboard.html", context)

@app.get("/clusters", response_class=HTMLResponse)
async def list_clusters(request: Request, user: dict = Depends(get_current_user)):
    request.state.now = datetime.now(timezone.utc)
    dashboard_data = get_dashboard_data(user)
    context = {"request": request, "user": user, **dashboard_data}
    return templates.TemplateResponse("clusters.html", context)

@app.get("/clusters/{account_id}/{region}/{cluster_name}", response_class=HTMLResponse)
async def read_cluster_detail(request: Request, account_id: str, region: str, cluster_name: str, user: dict = Depends(get_current_user)):
    cache_key = f"cluster_{account_id}_{region}_{cluster_name}"
    if cache_key in cache:
        logging.info(f"Cache HIT for cluster detail: {cluster_name}")
        cluster_details = cache[cache_key]
    else:
        try:
            self_account_id = boto3.client("sts").get_caller_identity().get("Account")
        except Exception as e:
            logging.warning(f"Unable to get self account: {e}")
            self_account_id = None

        role_arn = get_role_arn_for_account(account_id) if account_id != self_account_id else None
        if account_id != self_account_id and not role_arn:
            error_msg = f"No role ARN configured for account {account_id}"
            logging.error(error_msg)
            return templates.TemplateResponse("error.html", {"request": request, "errors": [error_msg]}, status_code=404)

        cluster_details = get_single_cluster_details(account_id, region, cluster_name, role_arn)
        if not cluster_details.get("errors"):
            cache[cache_key] = cluster_details

    context = {"request": request, "user": user, "cluster": cluster_details, "account_id": account_id, "region": region}
    return templates.TemplateResponse("cluster_detail.html", context)

@app.post("/api/refresh-data")
async def refresh_data(user: dict = Depends(get_current_user)):
    cache.clear()
    logging.info(f"Cache cleared by {user.get('email')}")
    return JSONResponse(content={"status": "success", "message": "Dashboard cache cleared."})

@app.post("/api/refresh-cluster/{account_id}/{region}/{cluster_name}")
async def refresh_cluster(account_id: str, region: str, cluster_name: str, user: dict = Depends(get_current_user)):
    cache_key = f"cluster_{account_id}_{region}_{cluster_name}"
    if cache_key in cache:
        del cache[cache_key]
        logging.info(f"Cache cleared for cluster {cluster_name}")
    return JSONResponse(content={"status": "success", "message": "Cluster cache cleared."})

@app.post("/api/upgrade-nodegroup")
async def upgrade_nodegroup_api(request: Request, user: dict = Depends(get_current_user)):
    data = await request.json()
    account_id = data.get("accountId")
    role_arn = get_role_arn_for_account(account_id)
    result = upgrade_nodegroup_version(account_id, data.get("region"), data.get("clusterName"), data.get("nodegroupName"), role_arn)
    return JSONResponse(content=result, status_code=400 if "error" in result else 200)

@app.get("/api/metrics/{account_id}/{region}/{cluster_name}")
async def get_metrics_api(account_id: str, region: str, cluster_name: str, user: dict = Depends(get_current_user)):
    role_arn = get_role_arn_for_account(account_id)
    metrics = get_cluster_metrics(account_id, region, cluster_name, role_arn)
    return JSONResponse(content=metrics, status_code=500 if "error" in metrics else 200)

# --- WebSocket: Logs ---
@app.websocket("/ws/logs/{account_id}/{region}/{cluster_name}/{namespace}/{pod_name}")
async def stream_logs(websocket: WebSocket, account_id: str, region: str, cluster_name: str, namespace: str, pod_name: str):
    await websocket.accept()
    role_arn = get_role_arn_for_account(account_id)
    try:
        cluster = get_single_cluster_details(account_id, region, cluster_name, role_arn)
        if cluster.get("errors"):
            await websocket.send_text(f"ERROR: {cluster['errors']}")
            return
        endpoint = cluster.get("endpoint")
        if not endpoint:
            raise ValueError("Missing cluster endpoint")

        api = get_k8s_api_client(cluster_name, endpoint, cluster['certificateAuthority']['data'], region, role_arn)
        core = client.CoreV1Api(api)
        pod = core.read_namespaced_pod(name=pod_name, namespace=namespace)
        container = pod.spec.containers[0].name

        stream_logs = stream.stream(core.read_namespaced_pod_log, name=pod_name, namespace=namespace, container=container, follow=True, _preload_content=False)
        while stream_logs.is_open():
            line = stream_logs.readline()
            if line:
                await websocket.send_text(line)
            else:
                await asyncio.sleep(0.1)
    except Exception as e:
        await websocket.send_text(f"ERROR: {e}")
        logging.error(f"Log stream error: {e}")
    finally:
        await websocket.close()

# --- WebSocket: Events ---
@app.websocket("/ws/events/{account_id}/{region}/{cluster_name}")
async def stream_events(websocket: WebSocket, account_id: str, region: str, cluster_name: str):
    await websocket.accept()
    role_arn = get_role_arn_for_account(account_id)
    w = None
    try:
        cluster = get_single_cluster_details(account_id, region, cluster_name, role_arn)
        if cluster.get("errors"):
            await websocket.send_text(json.dumps({"type": "ERROR", "message": cluster["errors"]}))
            return
        api = get_k8s_api_client(cluster_name, cluster["endpoint"], cluster["certificateAuthority"]["data"], region, role_arn)
        core = client.CoreV1Api(api)
        w = watch.Watch()

        for event in w.stream(core.list_event_for_all_namespaces, timeout_seconds=3600):
            obj = api.sanitize_for_serialization(event["object"])
            await websocket.send_text(json.dumps({"type": event["type"], "object": obj}))
    except WebSocketDisconnect:
        logging.info(f"Disconnected: {cluster_name}")
    except Exception as e:
        await websocket.send_text(json.dumps({"type": "ERROR", "message": str(e)}))
        logging.error(f"Event stream error: {e}")
    finally:
        if w:
            w.stop()
        await websocket.close()

# --- Entry Point ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True, ws="wsproto")
