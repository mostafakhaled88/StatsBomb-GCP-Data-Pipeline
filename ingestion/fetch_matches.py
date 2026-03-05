import logging
import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.config import (
    BUCKET_NAME,
    COMPETITIONS_ENDPOINT,
    MATCHES_ENDPOINT
)
from ingestion.gcs_client import upload_json

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_http_session():
    """Creates a requests session with a retry strategy."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,  # Waits 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def fetch_competitions(session):
    """Retrieves the master list of competitions."""
    logger.info("Fetching competitions list...")
    response = session.get(COMPETITIONS_ENDPOINT, timeout=30)
    response.raise_for_status()
    return response.json()

def fetch_matches():
    """Main execution logic for match ingestion."""
    session = get_http_session()
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    try:
        competitions = fetch_competitions(session)
    except Exception as e:
        logger.error(f"Critical failure fetching competitions: {e}")
        return

    for comp in competitions:
        comp_id = comp.get("competition_id")
        season_id = comp.get("season_id")
        
        if not comp_id or not season_id:
            continue

        url = f"{MATCHES_ENDPOINT}/{comp_id}/{season_id}.json"
        
        try:
            logger.info(f"Requesting matches for Comp {comp_id}, Season {season_id}")
            response = session.get(url, timeout=30)
            
            if response.status_code == 200:
                gcs_path = (
                    f"matches/"
                    f"competition_id={comp_id}/"
                    f"season_id={season_id}/"
                    f"ingestion_date={ingestion_date}/"
                    f"matches.json"
                )

                upload_json(
                    bucket_name=BUCKET_NAME,
                    destination_path=gcs_path,
                    data=response.content
                )
            else:
                logger.warning(f"Skipping {comp_id}-{season_id}: Received status {response.status_code}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching {comp_id}-{season_id}: {e}")
            continue

if __name__ == "__main__":
    fetch_matches()