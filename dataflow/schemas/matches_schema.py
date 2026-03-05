# dataflow/schemas/matches_schema.py

MATCHES_SCHEMA = {
    "fields": [
        {"name": "match_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "match_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "kick_off", "type": "TIME", "mode": "NULLABLE"},

        # Competition
        {"name": "competition_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "competition_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "competition_country", "type": "STRING", "mode": "NULLABLE"},

        # Season
        {"name": "season_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "season_name", "type": "STRING", "mode": "NULLABLE"},

        # Home team
        {"name": "home_team_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "home_team_name", "type": "STRING", "mode": "NULLABLE"},

        # Away team
        {"name": "away_team_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "away_team_name", "type": "STRING", "mode": "NULLABLE"},

        # Scores
        {"name": "home_score", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "away_score", "type": "INTEGER", "mode": "NULLABLE"},

        # Metadata
        {"name": "match_status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "match_week", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "stadium_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "referee_name", "type": "STRING", "mode": "NULLABLE"},

        # Audit
        {"name": "ingestion_date", "type": "DATE", "mode": "REQUIRED"},
    ]
}
