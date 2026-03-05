LINEUP_SCHEMA = {
    "fields": [
        {"name": "match_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "team_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "team_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "formation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "player_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "player_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "jersey_number", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "position_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "position_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingestion_date", "type": "DATE", "mode": "REQUIRED"},
        
    ]
}