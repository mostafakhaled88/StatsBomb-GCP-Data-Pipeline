# dataflow/utils/safe_get.py

def safe_get(dct, *keys):
    """
    Safely extract nested dictionary values.
    Returns None if any key is missing.
    """
    for key in keys:
        if not isinstance(dct, dict):
            return None
        dct = dct.get(key)
    return dct
