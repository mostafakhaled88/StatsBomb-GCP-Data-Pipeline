# ingestion/config.py
PROJECT_ID = "civil-hull-489201-q7"  
BUCKET_NAME = "statsbomb-raw-data"

BASE_URL = (
    "https://raw.githubusercontent.com/"
    "statsbomb/open-data/master/data"
)

COMPETITIONS_ENDPOINT = f"{BASE_URL}/competitions.json"
EVENTS_ENDPOINT = (
    "https://raw.githubusercontent.com/"
    "statsbomb/open-data/master/data/events"
)

MATCHES_ENDPOINT = f"{BASE_URL}/matches"

TIMEOUT = 30
