import logging
import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Ensure these imports match your folder structure
from ingestion.config import BUCKET_NAME, COMPETITIONS_ENDPOINT
from ingestion.gcs_client import upload_json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_http_session():
    """Configures a session with retry logic."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def fetch_competitions():
    """Fetches the static competition JSON and uploads to GCS."""
    session = get_http_session()
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info("Fetching static competitions list from StatsBomb...")

    try:
        # Static files do not support pagination; just make one request
        response = session.get(COMPETITIONS_ENDPOINT, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            logger.warning("No data retrieved from endpoint.")
            return

        # Hive-style partitioning path
        gcs_path = f"competitions/ingestion_date={ingestion_date}/competitions.json"

        # Upload the data
        upload_json(
            bucket_name=BUCKET_NAME,
            destination_path=gcs_path,
            data=data
        )
        logger.info(f"Successfully uploaded {len(data)} competitions to {gcs_path}")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")

if __name__ == "__main__":
    fetch_competitions()