# tests/test_api_schema.py
import requests
from ingestion.config import COMPETITIONS_ENDPOINT

def test_schema():
    response = requests.get(COMPETITIONS_ENDPOINT, timeout=10)
    data = response.json()
    
    if len(data) > 0:
        first_item = data[0]
        expected_keys = {"competition_id", "season_id", "competition_name"}
        if expected_keys.issubset(first_item.keys()):
            print("✅ API Schema matches expected format.")
        else:
            print(f"⚠️ Warning: Missing keys. Found: {list(first_item.keys())}")

if __name__ == "__main__":
    test_schema()