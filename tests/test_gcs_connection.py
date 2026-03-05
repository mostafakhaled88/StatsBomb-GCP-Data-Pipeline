# tests/test_gcs_connection.py
import json
from ingestion.gcs_client import upload_json
from ingestion.config import BUCKET_NAME

def test_upload():
    test_data = {"status": "success", "message": "Connection test"}
    path = "test/connection_check.json"
    
    try:
        upload_json(BUCKET_NAME, path, test_data)
        print("✅ GCS Connection Test Passed!")
    except Exception as e:
        print(f"❌ GCS Connection Test Failed: {e}")

if __name__ == "__main__":
    test_upload()