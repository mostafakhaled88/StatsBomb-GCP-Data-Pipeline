import logging
import requests
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import storage

from ingestion.config import BUCKET_NAME, EVENTS_ENDPOINT

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STORAGE_CLIENT = storage.Client()

def get_resilient_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def fetch_single_match(event_meta, ingestion_date, session):
    match_id = event_meta.get("match_id")
    comp_id = event_meta.get("competition_id")
    season_id = event_meta.get("season_id")
    
    url = f"{EVENTS_ENDPOINT}/{match_id}.json"
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()

        gcs_path = (
            f"events/competition_id={comp_id}/"
            f"season_id={season_id}/"
            f"match_id={match_id}/"
            f"ingestion_date={ingestion_date}/"
            f"events.json"
        )
        
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(response.content, content_type="application/json")
        
        return "SUCCESS"

    except Exception as e:
        logger.error(f"Error on match {match_id}: {str(e)}")
        return "FAILURE"

def fetch_events_parallel(matches, max_workers=10):
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    session = get_resilient_session()
    stats = Counter() # Track success vs failure
    
    logger.info(f"Starting parallel ingestion for {len(matches)} matches...")

    

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a dictionary to map futures to their match IDs for better error tracking
        future_to_match = {
            executor.submit(fetch_single_match, m, ingestion_date, session): m.get("match_id") 
            for m in matches
        }
        
        # as_completed yields futures as soon as they finish
        for future in as_completed(future_to_match):
            result = future.result()
            stats[result] += 1
    
    # --- The Status Report ---
    total = len(matches)
    success = stats["SUCCESS"]
    failed = stats["FAILURE"]
    success_rate = (success / total) * 100 if total > 0 else 0

    print("\n" + "="*30)
    print("      INGESTION REPORT      ")
    print("="*30)
    print(f"Total Matches:   {total}")
    print(f"Successfully:    {success} ✅")
    print(f"Failed:          {failed} ❌")
    print(f"Success Rate:    {success_rate:.2f}%")
    print("="*30 + "\n")

    if failed > 0:
        logger.warning(f"Pipeline finished with {failed} errors. Check logs for details.")

def load_matches_from_csv(file_path):
    import csv
    with open(file_path, mode='r') as f:
        return list(csv.DictReader(f))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--matches_csv", required=True)
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    matches_data = load_matches_from_csv(args.matches_csv)
    fetch_events_parallel(matches_data, max_workers=args.workers)