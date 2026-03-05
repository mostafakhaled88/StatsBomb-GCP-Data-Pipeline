import json
import re
import logging
import apache_beam as beam
from dataflow.utils.safe_get import safe_get

class ParseMatchesFn(beam.DoFn):
    def process(self, element):
        """
        element: Tuple of (file_path, file_content)
        """
        file_path, content = element
        
        # Extract ingestion_date from path: gs://.../ingestion_date=YYYY-MM-DD/...
        date_match = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", file_path)
        partition_date = date_match.group(1) if date_match else "1970-01-01"

        try:
            records = json.loads(content)
            for record in records:
                yield {
                    "match_id": record.get("match_id"),
                    "match_date": record.get("match_date"),
                    "kick_off": record.get("kick_off"),

                    # Competition (Nested)
                    "competition_id": safe_get(record, "competition", "competition_id"),
                    "competition_name": safe_get(record, "competition", "competition_name"),
                    "competition_country": safe_get(record, "competition", "country_name"),

                    # Season (Nested)
                    "season_id": safe_get(record, "season", "season_id"),
                    "season_name": safe_get(record, "season", "season_name"),

                    # Teams (Nested)
                    "home_team_id": safe_get(record, "home_team", "home_team_id"),
                    "home_team_name": safe_get(record, "home_team", "home_team_name"),
                    "away_team_id": safe_get(record, "away_team", "away_team_id"),
                    "away_team_name": safe_get(record, "away_team", "away_team_name"),

                    "home_score": record.get("home_score"),
                    "away_score": record.get("away_score"),

                    "match_status": record.get("match_status"),
                    "match_week": record.get("match_week"),
                    "stadium_name": safe_get(record, "stadium", "name"),
                    "referee_name": safe_get(record, "referee", "name"),

                    "ingestion_date": partition_date,
                }
        except Exception as e:
            logging.error(f"Error parsing match file {file_path}: {e}")