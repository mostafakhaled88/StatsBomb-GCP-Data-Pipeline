# Dataflow Pipelines

This folder contains **Apache Beam pipelines** responsible for ingesting raw StatsBomb JSON data from Google Cloud Storage (GCS), parsing it into structured records, and loading it into **BigQuery** tables.

Each pipeline represents a **single logical data source** and can be run independently.

---

## Pipelines Overview

```
pipelines/
├── competitions_pipeline.py
├── matches_pipeline.py
├── events_pipeline.py
```

---

## competitions_pipeline.py

### Purpose

Ingests **competition-level metadata** from StatsBomb and loads it into BigQuery.

### Input

* **GCS Path**:

  ```
  gs://statsbomb-raw/competitions/*/competitions.json
  ```

### Output

* **BigQuery Table**:

  ```
  football-analytics-project.statsbomb_raw.competitions
  ```

### Processing Steps

1. Match competition JSON files in GCS
2. Read file contents as UTF-8
3. Parse records using `ParseCompetitionsFn`
4. Append structured rows to BigQuery using `COMPETITIONS_SCHEMA`

### Notes

* Uses `DirectRunner` by default (local execution)
* Automatically creates the BigQuery table if it does not exist

---

## matches_pipeline.py

### Purpose

Processes **match-level metadata** (fixtures, teams, dates, scores) and loads it into BigQuery.

### Input

* **GCS Path**:

  ```
  gs://statsbomb-raw/matches/**/matches.json
  ```

### Output

* **BigQuery Table**:

  ```
  football-analytics-project.statsbomb_raw.matches
  ```

### Processing Steps

1. Match match JSON files in nested folders
2. Read raw file content
3. Parse records using `ParseMatchesFn`
4. Append results to BigQuery using `MATCHES_SCHEMA`

### Notes

* Designed for append-only ingestion
* Schema enforcement happens at BigQuery write time

---

## events_pipeline.py

### Purpose

Processes **event-level match data**, including:

* Match events (passes, shots, duels, etc.)
* Team lineups
* Error handling and audit logging

This is the **most complex and performance-critical pipeline**.

### Inputs (CLI Arguments)

| Argument             | Description                        |
| -------------------- | ---------------------------------- |
| `--input_path`       | GCS path to match event JSON files |
| `--output_table`     | BigQuery events table              |
| `--lineup_table`     | BigQuery lineups table             |
| `--deadletter_table` | Parsing error (dead-letter) table  |
| `--bq_errors_table`  | BigQuery insert errors table       |

### Outputs

* Valid events → Events BigQuery table
* Lineups → Lineups BigQuery table
* Parsing failures → Dead-letter BigQuery table
* BigQuery insert failures → Audit table

### Processing Flow

1. Read event JSON files from GCS
2. Reshuffle data for worker load balancing
3. Parse records using `ParseEventsFn` with **multiple outputs**:

   * `valid` (events)
   * `lineups`
   * `failed` (parsing errors)
4. Write valid records using **streaming inserts**
5. Capture and persist BigQuery insert failures

### Reliability Features

* Dead-letter pattern for malformed JSON
* BigQuery insert failure capture (`FailedRows`)
* Append-only, idempotent design

---

## Design Principles

* **Single Responsibility**: One pipeline per data domain
* **Schema-first ingestion**: All writes use explicit BigQuery schemas
* **Fault tolerance**: Parsing and insertion errors are captured, not dropped
* **Scalability-ready**: Compatible with DataflowRunner for production

---

## How to Run

### Local (DirectRunner)

```bash
python dataflow/pipelines/competitions_pipeline.py
python dataflow/pipelines/matches_pipeline.py
```

### Dataflow (Example)

```bash
python dataflow/pipelines/events_pipeline.py \
  --runner DataflowRunner \
  --project football-analytics-project \
  --region us-central1 \
  --temp_location gs://statsbomb-raw/temp \
  --staging_location gs://statsbomb-raw/staging \
  --input_path gs://statsbomb-raw/events/**/*.json \
  --output_table football-analytics-project:statsbomb_raw.events \
  --lineup_table football-analytics-project:statsbomb_raw.lineups \
  --deadletter_table football-analytics-project:statsbomb_raw.events_deadletter \
  --bq_errors_table football-analytics-project:statsbomb_raw.bq_insert_errors
```

---

## Future Enhancements

* Partitioned & clustered BigQuery tables
* Data quality metrics (row counts, null checks)
* Unit tests for pipeline transforms
* Metadata-driven pipeline configuration

---

Part of the **Sports Data Pipeline** project.
