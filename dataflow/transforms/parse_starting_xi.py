import json
import logging
from datetime import datetime
import apache_beam as beam


class ParseLineupsFn(beam.DoFn):
    """
    Production-ready parser for StatsBomb Lineup JSON.
    Outputs one row per player-position.
    """

    def _safe_int(self, value):
        try:
            return int(value) if value is not None else None
        except (ValueError, TypeError):
            return None

    def process(self, element, match_id):
        ingestion_date = datetime.utcnow().date().isoformat()

        try:
            data = json.loads(element)

            if not isinstance(data, list):
                raise ValueError("Lineup JSON is not a list")

            for team in data:

                team_id = self._safe_int(team.get("team_id"))
                team_name = team.get("team_name")
                formation = team.get("formation") or None

                # Skip invalid teams
                if team_id is None:
                    continue

                for player in team.get("lineup", []):

                    player_id = self._safe_int(player.get("player_id"))
                    player_name = player.get("player_name")
                    jersey_number = self._safe_int(player.get("jersey_number"))
                    positions = player.get("positions") or []

                    # Skip invalid players
                    if player_id is None:
                        continue

                    # -----------------------------------------
                    # CASE 1: UNUSED SUBSTITUTE
                    # -----------------------------------------
                    if not positions:
                        yield {
                            "match_id": self._safe_int(match_id),
                            "team_id": team_id,
                            "team_name": team_name,
                            "formation": formation,
                            "player_id": player_id,
                            "player_name": player_name,
                            "jersey_number": jersey_number,
                            "position_id": None,
                            "position_name": "Unused Sub",
                            "ingestion_date": ingestion_date
                        }
                        continue

                    # -----------------------------------------
                    # CASE 2: PLAYER HAS POSITION ENTRIES
                    # -----------------------------------------
                    for pos in positions:
                        yield {
                            "match_id": self._safe_int(match_id),
                            "team_id": team_id,
                            "team_name": team_name,
                            "formation": formation,
                            "player_id": player_id,
                            "player_name": player_name,
                            "jersey_number": jersey_number,
                            "position_id": self._safe_int(pos.get("position_id")),
                            "position_name": pos.get("position"),
                            "ingestion_date": ingestion_date
                        }

        except Exception as e:
            logging.exception("Lineup parsing failed")

            yield beam.pvalue.TaggedOutput(
                "failed",
                {
                    "match_id": match_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )