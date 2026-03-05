CREATE OR REPLACE TABLE `statsbomb_raw.gold_player_match_stats` AS
WITH finals AS (
  SELECT match_id FROM `statsbomb_raw.matches`
  WHERE competition_name = 'Champions League'
  AND match_id IN (22912, 18235, 18236, 18237, 18240, 18241, 18242, 18243, 18244, 18245)
),

-- Step 1: Aggregate Events
player_events AS (
  SELECT
    e.match_id, e.player_id, e.player_name, e.team_id, e.team_name,
    COUNTIF(e.event_type = 'Shot') AS shots,
    COUNTIF(e.event_type = 'Pass') AS total_passes,
    COUNTIF(e.event_type = 'Pressure') AS pressures,
    COUNTIF(e.event_type = 'Foul Committed') AS fouls,
    COUNTIF(e.event_type = 'Pass' AND e.location_x > 80) AS attacking_third_passes,
    COUNTIF(e.event_type = 'Dribble') AS successful_dribbles
  FROM `statsbomb_raw.events` e
  JOIN finals f ON e.match_id = f.match_id
  WHERE e.player_id IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
)

-- Step 2: Join with Lineup Metadata
SELECT
  pe.*,
  -- We use COALESCE to provide "Substitute" if position is missing from lineups table
  COALESCE(l.formation, 'Unknown') AS formation,
  COALESCE(l.position_name, 'Substitute / Tactical Shift') AS position_name,
  l.jersey_number,
  TRUE AS match_appearance
FROM player_events pe
LEFT JOIN `statsbomb_raw.lineups` l
  ON pe.match_id = l.match_id
  AND pe.player_id = l.player_id
  AND pe.team_id = l.team_id;