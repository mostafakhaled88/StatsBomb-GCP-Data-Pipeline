# dataflow/schemas/competitions_schema.py

COMPETITIONS_SCHEMA = {
    "fields": [
        {"name": "competition_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "season_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "competition_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "season_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "competition_gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "competition_youth", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "competition_international", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "ingestion_date", "type": "DATE", "mode": "REQUIRED"},
    ]
}
