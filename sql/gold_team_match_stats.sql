CREATE OR REPLACE TABLE statsbomb_raw.gold_team_match_stats AS
WITH finals AS (
  SELECT *
  FROM statsbomb_raw.matches
  WHERE competition_name = 'Champions League'
    AND match_id IN (
      22912, 18235, 18236, 18237, 18240,
      18241, 18242, 18243, 18244, 18245
    )
),

team_events AS (
  SELECT
    e.match_id,
    e.team_id,
    e.team_name,

    COUNTIF(e.event_type = 'Shot') AS shots,
    COUNTIF(e.event_type = 'Pass') AS passes,
    COUNTIF(e.event_type = 'Pressure') AS pressures,
    COUNTIF(e.event_type = 'Foul Committed') AS fouls,

    COUNT(*) AS total_events
  FROM statsbomb_raw.events e
  JOIN finals f
    ON e.match_id = f.match_id
  GROUP BY
    e.match_id,
    e.team_id,
    e.team_name
),

match_totals AS (
  SELECT
    match_id,
    SUM(total_events) AS match_events
  FROM team_events
  GROUP BY match_id
)

SELECT
  te.match_id,
  
  -- ✅ New columns added from the 'finals' CTE
  f.match_date,
  f.stadium_name,

  te.team_id,
  te.team_name,

  -- ✅ Correct goals logic
  CASE
    WHEN te.team_id = f.home_team_id THEN f.home_score
    WHEN te.team_id = f.away_team_id THEN f.away_score
  END AS goals,

  te.shots,
  te.passes,
  te.pressures,
  te.fouls,

  ROUND(te.total_events / mt.match_events * 100, 2) AS possession_pct
FROM team_events te
JOIN match_totals mt
  ON te.match_id = mt.match_id
JOIN finals f
  ON te.match_id = f.match_id;