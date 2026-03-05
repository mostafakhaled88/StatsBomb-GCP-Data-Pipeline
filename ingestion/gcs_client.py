# ingestion/gcs_client.py

import json
import logging
from google.cloud import storage

# Configure logging to be consistent with your other scripts
logger = logging.getLogger(__name__)

# Global client for thread-safe reuse
_STORAGE_CLIENT = None

def get_storage_client():
    """Returns a singleton GCS client to avoid overhead."""
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is None:
        _STORAGE_CLIENT = storage.Client()
    return _STORAGE_CLIENT

def upload_json(bucket_name: str, destination_path: str, data):
    """
    Uploads data to GCS. 
    Handles both 'bytes' (raw API response) and 'dict/list' (Python objects).
    """
    try:
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_path)

        # Handle different data types automatically
        if isinstance(data, (dict, list)):
            # Convert Python objects to a JSON string
            content = json.dumps(data, indent=2)
            content_type = "application/json"
            blob.upload_from_string(content, content_type=content_type)
        else:
            # Assume data is already bytes/string (response.content)
            blob.upload_from_string(data, content_type="application/json")

        logger.info(f"Successfully uploaded: gs://{bucket_name}/{destination_path}")
        
    except Exception as e:
        logger.error(f"Failed to upload to {destination_path}: {e}")
        raise  # Re-raise to let the main script know the upload failed