import asyncio
import logging
from sqlalchemy.orm import Session
from datetime import datetime

from database import SessionLocal, DataUpdateLog, update_cluster_data, get_all_clusters_summary
from aws_data_fetcher import get_live_eks_data, get_single_cluster_details, get_role_arn_for_account, _process_cluster_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def update_all_data():
    """Fetches all cluster data from AWS and updates the database."""
    db_session: Session = SessionLocal()
    logging.info("Starting background data update...")
    try:
        # 1. Fetch raw cluster list from AWS
        raw_data = get_live_eks_data()
        if raw_data.get("errors"):
            logging.error(f"Errors during cluster discovery: {raw_data['errors']}")

        clusters_from_aws = raw_data.get("clusters", [])
        logging.info(f"Discovered {len(clusters_from_aws)} clusters from AWS APIs.")

        # 2. Get clusters currently in our DB to handle deletions
        clusters_in_db = { (c['account_id'], c['region'], c['name']) for c in get_all_clusters_summary(db_session) }
        clusters_from_aws_set = set()

        tasks = []
        for c_raw in clusters_from_aws:
            # FIX: Extract account_id and other necessary info from the raw data
            try:
                account_id = c_raw["arn"].split(':')[4]
                region = c_raw["region"]
                name = c_raw["name"]
                
                clusters_from_aws_set.add((account_id, region, name))

                # Create an async task for each cluster update
                tasks.append(update_single_cluster_data(db_session, account_id, region, name))
            except (KeyError, IndexError) as e:
                logging.error(f"Could not parse essential info from raw cluster data: {c_raw}. Error: {e}")
                continue
        
        # 3. Process all updates concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        fail_count = len(results) - success_count
        
        # 4. Handle deletions: clusters in DB but not in AWS anymore
        clusters_to_delete = clusters_in_db - clusters_from_aws_set
        if clusters_to_delete:
            logging.info(f"Found {len(clusters_to_delete)} clusters to delete.")
            for acc_id, reg, clus_name in clusters_to_delete:
                cluster_to_delete = db_session.query(database.Cluster).filter_by(account_id=acc_id, region=reg, name=clus_name).first()
                if cluster_to_delete:
                    db_session.delete(cluster_to_delete)
                    logging.info(f"Deleted stale cluster record: {clus_name} in {reg}")
            db_session.commit()

        details_message = f"Update complete. Success: {success_count}, Failed: {fail_count}, Deleted: {len(clusters_to_delete)}."
        logging.info(details_message)
        
        log_entry = DataUpdateLog(status='SUCCESS', details=details_message)
        db_session.add(log_entry)
        db_session.commit()

    except Exception as e:
        logging.error(f"Fatal error during background data update: {e}", exc_info=True)
        log_entry = DataUpdateLog(status='FAIL', details=str(e))
        db_session.add(log_entry)
        db_session.commit()
    finally:
        db_session.close()

async def update_single_cluster_data(db_session: Session, account_id: str, region: str, cluster_name: str):
    """Fetches and updates data for a single cluster."""
    logging.info(f"Updating details for cluster: {cluster_name} in {region}")
    try:
        role_arn = get_role_arn_for_account(account_id)
        # This function from aws_data_fetcher already returns a rich dictionary
        detailed_data = get_single_cluster_details(account_id, region, cluster_name, role_arn)
        
        if detailed_data and not detailed_data.get("errors"):
            # The update_cluster_data function in database.py handles the DB transaction
            update_cluster_data(db_session, detailed_data)
            logging.info(f"Successfully updated {cluster_name}.")
        elif detailed_data and detailed_data.get("errors"):
             raise Exception(f"Failed to get details for {cluster_name}: {detailed_data.get('errors')}")
        else:
             raise Exception(f"Received no data for {cluster_name}")
    except Exception as e:
        logging.error(f"Could not update cluster {cluster_name}: {e}", exc_info=False) # exc_info=False to reduce noise from expected auth errors
        raise
