EVENTS_SCHEMA = {
    "fields": [
        {"name": "match_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "index", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "period", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "minute", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "second", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
        {"name": "team_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "team_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "player_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "player_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_type_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "possession", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "play_pattern", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingestion_date", "type": "DATE", "mode": "REQUIRED"},
        
        # --- Recommended Additions ---
        {"name": "location_x", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "location_y", "type": "FLOAT", "mode": "NULLABLE"},
        # Useful for unexpected fields without breaking the pipeline
        {"name": "metadata_file_path", "type": "STRING", "mode": "NULLABLE"} 
    ]
}