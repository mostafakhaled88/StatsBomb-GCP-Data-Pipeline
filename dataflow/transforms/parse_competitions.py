import json
import re
import logging
import apache_beam as beam

class ParseCompetitionsFn(beam.DoFn):
    def process(self, element):
        """
        element: Tuple of (file_path, file_content)
        """
        file_path, content = element
        
        # 1. Extract ingestion_date from path (Hive partition)
        # Regex looks for 'ingestion_date=YYYY-MM-DD'
        date_match = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", file_path)
        partition_date = date_match.group(1) if date_match else "1970-01-01"

        try:
            records = json.loads(content)
            for record in records:
                # 2. Structured Mapping with Type Safety
                yield {
                    "competition_id": int(record.get("competition_id", 0)),
                    "season_id": int(record.get("season_id", 0)),
                    "competition_name": str(record.get("competition_name", "")),
                    "country_name": str(record.get("country_name", "")),
                    "season_name": str(record.get("season_name", "")),
                    "competition_gender": str(record.get("competition_gender", "unknown")),
                    "competition_youth": bool(record.get("competition_youth", False)),
                    "competition_international": bool(record.get("competition_international", False)),
                    "ingestion_date": partition_date, # From path, not system clock
                }
        except Exception as e:
            logging.error(f"Failed to parse file {file_path}: {e}")