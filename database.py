import os
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, ForeignKey, Boolean, Text
from sqlalchemy.orm import sessionmaker, relationship, Session, joinedload
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./eks_dashboard.db")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- ORM Models (unchanged) ---
class Cluster(Base):
    __tablename__ = "clusters"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    account_id = Column(String, index=True, nullable=False)
    region = Column(String, index=True, nullable=False)
    arn = Column(String, unique=True)
    version = Column(String)
    platform_version = Column(String)
    endpoint = Column(String)
    status = Column(String)
    created_at = Column(DateTime)
    role_arn = Column(String)
    tags = Column(JSON)
    health_issues = Column(JSON)
    eks_auto_mode = Column(String)
    certificate_authority_data = Column(Text)
    networking = Column(JSON)
    oidc_provider_url = Column(String)
    security_insights = Column(JSON)
    workloads = Column(JSON)
    last_updated = Column(DateTime, default=datetime.utcnow)
    nodegroups = relationship("Nodegroup", back_populates="cluster", cascade="all, delete-orphan")
    addons = relationship("Addon", back_populates="cluster", cascade="all, delete-orphan")
    fargate_profiles = relationship("FargateProfile", back_populates="cluster", cascade="all, delete-orphan")

class Nodegroup(Base):
    __tablename__ = "nodegroups"
    id = Column(Integer, primary_key=True, index=True)
    cluster_id = Column(Integer, ForeignKey("clusters.id"))
    name = Column(String)
    status = Column(String)
    ami_type = Column(String)
    instance_types = Column(JSON)
    release_version = Column(String)
    version = Column(String)
    created_at = Column(DateTime)
    desired_size = Column(Integer)
    is_karpenter_node = Column(Boolean, default=False)
    cluster = relationship("Cluster", back_populates="nodegroups")

class Addon(Base):
    __tablename__ = "addons"
    id = Column(Integer, primary_key=True, index=True)
    cluster_id = Column(Integer, ForeignKey("clusters.id"))
    name = Column(String)
    version = Column(String)
    status = Column(String)
    pod_identity_display = Column(String)
    irsa_role_arn = Column(String)
    cluster = relationship("Cluster", back_populates="addons")

class FargateProfile(Base):
    __tablename__ = "fargate_profiles"
    id = Column(Integer, primary_key=True, index=True)
    cluster_id = Column(Integer, ForeignKey("clusters.id"))
    name = Column(String)
    status = Column(String)
    cluster = relationship("Cluster", back_populates="fargate_profiles")

class RequestLog(Base):
    __tablename__ = "request_logs"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    method = Column(String)
    url = Column(String)
    client_ip = Column(String)
    request_size = Column(Integer)
    response_size = Column(Integer)
    response_status = Column(Integer)

class DataUpdateLog(Base):
    __tablename__ = "data_update_logs"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    status = Column(String)
    details = Column(Text)

# --- DB Setup & Utils (unchanged) ---
def create_db_and_tables(): Base.metadata.create_all(bind=engine)
def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()
def get_last_update_time(session: Session):
    last_success = session.query(DataUpdateLog).filter(DataUpdateLog.status == 'SUCCESS').order_by(DataUpdateLog.timestamp.desc()).first()
    return last_success.timestamp if last_success else None
def log_request(session: Session, method: str, url: str, client_ip: str, request_size: int, response_size: int, response_status: int):
    log_entry = RequestLog(method=method, url=url, client_ip=client_ip, request_size=request_size, response_size=response_size, response_status=response_status)
    session.add(log_entry)
    session.commit()

# --- CRUD Operations ---
def update_cluster_data(session: Session, cluster_data: dict):
    cluster = session.query(Cluster).filter(
        Cluster.account_id == cluster_data.get('account_id'),
        Cluster.region == cluster_data.get('region'),
        Cluster.name == cluster_data.get('name')
    ).first()

    if not cluster:
        cluster = Cluster(
            name=cluster_data['name'],
            account_id=cluster_data['account_id'],
            region=cluster_data['region']
        )
        session.add(cluster)

    # --- Cluster attributes ---
    cluster.arn = cluster_data.get('arn')
    cluster.version = cluster_data.get('version')
    cluster.platform_version = cluster_data.get('platformVersion')
    cluster.endpoint = cluster_data.get('endpoint')
    cluster.status = cluster_data.get('status')

    created_at = cluster_data.get('createdAt')
    if isinstance(created_at, str):
        cluster.created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
    elif isinstance(created_at, datetime):
        cluster.created_at = created_at

    cluster.role_arn = cluster_data.get('roleArn', 'Null')
    if cluster.role_arn and isinstance(cluster.role_arn, str):
        cluster.role_arn = cluster.role_arn.strip()

    cluster.tags = cluster_data.get('tags')
    cluster.health_issues = cluster_data.get('health_issues')
    cluster.eks_auto_mode = cluster_data.get('eks_auto_mode')
    cluster.networking = cluster_data.get('networking')
    cluster.oidc_provider_url = cluster_data.get('oidc_provider_url')
    cluster.security_insights = cluster_data.get('security_insights')
    cluster.workloads = cluster_data.get('workloads')

    if cluster_data.get('certificateAuthority'):
        cluster.certificate_authority_data = cluster_data['certificateAuthority'].get('data')

    cluster.last_updated = datetime.utcnow()

    # --- Subresources ---
    _update_sub_resources(
        session, cluster, 'nodegroups', 'name',
        cluster_data.get('nodegroups_data', []),
        lambda ng: {
            'name': ng.get('name'),
            'status': ng.get('status'),
            'ami_type': ng.get('amiType'),
            'instance_types': ng.get('instanceTypes'),
            'release_version': ng.get('releaseVersion'),
            'version': ng.get('version'),
            'created_at': ng.get('createdAt'),
            'desired_size': ng.get('desiredSize'),
            'is_karpenter_node': ng.get('is_karpenter_node', False)
        }
    )

    _update_sub_resources(
        session, cluster, 'addons', 'name',
        cluster_data.get('addons', []),
        lambda a: {
            'name': a.get('addonName'),
            'version': a.get('addonVersion'),
            'status': a.get('status'),
            'pod_identity_display': a.get('pod_identity_display'),
            'irsa_role_arn': a.get('irsa_role_arn')
        }
    )

    _update_sub_resources(
        session, cluster, 'fargate_profiles', 'name',
        cluster_data.get('fargate_profiles', [])
    )

    # --- Debug print for verification ---
    print("[DEBUG] Final Cluster object before commit:")
    print("  role_arn:", cluster.role_arn)
    print("  nodegroups:", json.dumps(cluster_data.get('nodegroups_data', []), indent=2, default=str))
    print("  addons:", json.dumps(cluster_data.get('addons', []), indent=2, default=str))

    session.commit()
    session.refresh(cluster)
    return cluster

def _update_sub_resources(session: Session, parent_cluster: Cluster, relationship_name: str, key_field: str, incoming_data: list, data_transformer=None):
    existing_items = getattr(parent_cluster, relationship_name)
    incoming_key_field = 'name' if relationship_name != 'addons' else 'addonName'
    
    existing_map = {getattr(item, key_field): item for item in existing_items}
    incoming_map = {item[incoming_key_field]: item for item in incoming_data}

    # Delete old items not in the new data
    for key, item in existing_map.items():
        if key not in incoming_map:
            session.delete(item)
            
    # Add or Update items
    for key, data in incoming_map.items():
        orm_class = getattr(Cluster, relationship_name).property.mapper.class_
        if data_transformer:
            data = data_transformer(data)

        if key in existing_map:
            item = existing_map[key]
            for field, value in data.items():
                if hasattr(item, field):
                    setattr(item, field, value)
        else:
            valid_fields = {c.name for c in orm_class.__table__.columns}
            filtered_data = {k: v for k, v in data.items() if k in valid_fields}
            
            new_item = orm_class(cluster_id=parent_cluster.id, **filtered_data)
            session.add(new_item)

def get_all_clusters_summary(session: Session):
    clusters = session.query(Cluster).all()
    return [{
        "name": c.name, "account_id": c.account_id, "region": c.region, "version": c.version, "status": c.status,
        "createdAt": c.created_at.isoformat() if c.created_at else None,
        "health_status_summary": "HEALTHY" if not c.health_issues else "HAS_ISSUES",
        "upgrade_insight_status": "PASSING" if c.version and c.version >= "1.29" else "NEEDS_ATTENTION",
    } for c in clusters]

def get_cluster_details(session: Session, account_id: str, region: str, cluster_name: str):
    cluster = session.query(Cluster).options(
        joinedload(Cluster.nodegroups),
        joinedload(Cluster.addons),
        joinedload(Cluster.fargate_profiles)
    ).filter(
        Cluster.account_id == account_id,
        Cluster.region == region,
        Cluster.name == cluster_name
    ).first()

    if not cluster:
        return None

    # Exclude the 'workloads' field from the bulk dict for custom handling
    cluster_dict = {
        key: getattr(cluster, key)
        for key in cluster.__table__.columns.keys()
        if key != 'workloads'
    }

    cluster_dict['createdAt'] = cluster.created_at
    cluster_dict['certificateAuthority'] = {'data': cluster.certificate_authority_data}
    cluster_dict['nodegroups_data'] = [vars(ng) for ng in cluster.nodegroups]
    cluster_dict['addons'] = [
        {
            'addonName': a.name,
            'addonVersion': a.version,
            'status': a.status,
            'pod_identity_display': a.pod_identity_display,
            'irsa_role_arn': a.irsa_role_arn
        } for a in cluster.addons
    ]
    cluster_dict['fargate_profiles'] = [
        {'name': f.name, 'status': f.status}
        for f in cluster.fargate_profiles
    ]

    # Workloads
    cluster_dict['workloads'] = cluster.workloads

    # Error handling for UI
    if cluster.workloads and cluster.workloads.get('error'):
        cluster_dict['workloads_error'] = cluster.workloads.get('error')
    else:
        cluster_dict['workloads_error'] = None

    return cluster_dict

