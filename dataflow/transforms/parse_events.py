from dataflow.utils.filename_parser import parse_gcs_path
from dataflow.utils.safe_get import safe_get
import json
from datetime import datetime
import apache_beam as beam
from apache_beam.metrics import Metrics

class ParseEventsFn(beam.DoFn):
    def __init__(self):
        self.events_processed = Metrics.counter(self.__class__, "events_processed")
        self.lineups_processed = Metrics.counter(self.__class__, "lineups_processed")
        self.files_failed = Metrics.counter(self.__class__, "files_failed")

    def process(self, element):
        filename, file_contents = element
        
        try:
            # 1. Metadata Extraction
            metadata = parse_gcs_path(filename)
            match_id = metadata.get("match_id")
            raw_date = metadata.get("ingestion_date")

            if not match_id or not raw_date:
                raise ValueError(f"Missing metadata in path: {filename}")

            # Ensure date is in YYYY-MM-DD for BQ DATE type
            ingestion_date_str = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            
            # 2. JSON Parsing
            events = json.loads(file_contents)

            for event in events:
                event_type = safe_get(event, "type", "name")

                # --- Handle Starting XI (Lineups) ---
                if event_type == "Starting XI":
                    formation = safe_get(event, "tactics", "formation")
                    lineup = safe_get(event, "tactics", "lineup") or []
                    
                    for player_entry in lineup:
                        self.lineups_processed.inc()
                        yield beam.pvalue.TaggedOutput("lineups", {
                            "match_id": int(match_id),
                            "team_id": safe_get(event, "team", "id"),
                            "team_name": safe_get(event, "team", "name"),
                            "formation": formation,
                            "player_id": safe_get(player_entry, "player", "id"),
                            "player_name": safe_get(player_entry, "player", "name"),
                            "jersey_number": player_entry.get("jersey_number"),
                            "position_id": safe_get(player_entry, "position", "id"),
                            "position_name": safe_get(player_entry, "position", "name"),
                            "ingestion_date": ingestion_date_str
                        })
                    continue 

                # --- Handle Standard Events ---
                self.events_processed.inc()
                
                # Extracting coordinates and explicitly casting to float
                location = event.get("location", [])
                
                # Explicit casting to float prevents BQ from seeing them as strings/objects
                x = float(location[0]) if len(location) > 0 and location[0] is not None else None
                y = float(location[1]) if len(location) > 1 and location[1] is not None else None

                yield {
                    "match_id": int(match_id),
                    "event_id": event.get("id"),
                    "index": int(event.get("index", 0)), # Cast to int
                    "event_type": str(event_type),
                    "event_type_id": safe_get(event, "type", "id"),
                    "ingestion_date": ingestion_date_str,
                    "period": event.get("period"),
                    "minute": event.get("minute"),
                    "second": event.get("second"),
                    "timestamp": event.get("timestamp"),
                    "team_id": safe_get(event, "team", "id"),
                    "team_name": safe_get(event, "team", "name"),
                    "player_id": safe_get(event, "player", "id"),
                    "player_name": safe_get(event, "player", "name"),
                    "possession": event.get("possession"),
                    "play_pattern": safe_get(event, "play_pattern", "name"),
                    "location_x": x,
                    "location_y": y
                }

        except Exception as e:
            self.files_failed.inc()
            yield beam.pvalue.TaggedOutput("failed", {
                "file_name": filename,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })