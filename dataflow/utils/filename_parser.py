import re
import logging

# Pre-compile patterns for performance (compiled once when worker starts)
PATTERNS = {
    "competition_id": re.compile(r"competition_id=(\d+)"),
    "season_id": re.compile(r"season_id=(\d+)"),
    "match_id": re.compile(r"match_id=(\d+)"),
    "ingestion_date": re.compile(r"ingestion_date=(\d{4}-\d{2}-\d{2})"),
}

def parse_gcs_path(path: str) -> dict:
    """
    Extracts partition values from StatsBomb GCS paths safely.
    """
    extracted = {}
    missing_keys = []

    for key, pattern in PATTERNS.items():
        match = pattern.search(path)
        if match:
            val = match.group(1)
            extracted[key] = val if key == "ingestion_date" else int(val)
        else:
            missing_keys.append(key)

    if missing_keys:
        # Log a warning instead of raising an immediate crash
        # This allows the caller (ParseEventsFn) to decide if it should fail
        logging.warning(f"Metadata missing {missing_keys} in path: {path}")
        # Return a flag so the pipeline can route this to Dead-Letter
        extracted["is_valid"] = False
    else:
        extracted["is_valid"] = True

    return extracted