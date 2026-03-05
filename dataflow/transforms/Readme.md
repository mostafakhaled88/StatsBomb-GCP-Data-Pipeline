# Dataflow Transforms

This folder contains **Apache Beam `DoFn` transformation logic** used by the Dataflow pipelines. Transforms are responsible for parsing raw StatsBomb JSON files into **clean, structured records** ready for BigQuery ingestion.

All transforms are designed to be:

* **Stateless**
* **Reusable** across pipelines
* **Free of I/O logic** (no reads/writes)

---

## Overview

```
transforms/
├── parse_competitions.py
├── parse_matches.py
├── parse_events.py
```

Each file contains one or more `beam.DoFn` classes that handle a specific data domain.

---

## parse_competitions.py

### `ParseCompetitionsFn`

#### Purpose

Parses StatsBomb **competitions metadata** from JSON files and emits one row per competition-season.

#### Input

* Full JSON file content (string)
* JSON structure: list of competition records

#### Output Fields (BigQuery-ready)

* `competition_id`
* `season_id`
* `competition_name`
* `country_name`
* `season_name`
* `competition_gender`
* `competition_youth`
* `competition_international`
* `ingestion_date`

#### Key Features

* Handles list-based JSON input
* Automatically adds ingestion date (UTC)
* Defensive `.get()` access for optional fields

---

## parse_matches.py

### `ParseMatchesFn`

#### Purpose

Parses **match-level metadata** such as teams, scores, dates, and competition context.

#### Input

* Full JSON file content (string)
* JSON structure: list of matches

#### Output Fields

* Match identifiers and timestamps
* Competition and season metadata
* Home / away team details
* Scores, stadium, referee
* `ingestion_date`

#### Key Features

* Uses `safe_get` utility for nested JSON access
* Emits one row per match
* BigQuery DATE-compatible ingestion date

---

### `ParseStartingXIFn`

> ⚠️ Legacy / Optional Transform

#### Purpose

Extracts **starting XI lineups** from event JSON files.

#### Notes

* Functionality is now **superseded by `ParseEventsFn`**, which handles lineups and events together
* Retained for reference or future modularization

---

## parse_events.py

### `ParseEventsFn`

#### Purpose

Parses **event-level match data** and extracts:

* Match events (passes, shots, duels, etc.)
* Starting XI lineups
* Parsing errors (dead-letter records)

This transform powers the **core events pipeline**.

---

### Inputs

* Tuple: `(filename, file_contents)`
* Filename is used to extract metadata (match_id, ingestion_date)

---

### Outputs (Multi-Output DoFn)

| Output Tag | Description             |
| ---------- | ----------------------- |
| `valid`    | Parsed match events     |
| `lineups`  | Starting XI lineup rows |
| `failed`   | Parsing / logic errors  |

---

### Key Features

#### 1. Metadata Extraction

* Extracts `match_id` and `ingestion_date` from GCS path
* Validates required metadata early

#### 2. Robust JSON Parsing

* Uses `safe_get` for nested fields
* Explicit type casting for BigQuery compatibility
* Handles missing or optional fields gracefully

#### 3. Starting XI Handling

* Detects `Starting XI` events
* Emits one row per player
* Includes formation, team, and player metadata

#### 4. Metrics & Observability

Uses Apache Beam Metrics:

* `events_processed`
* `lineups_processed`
* `files_failed`

These metrics are visible in the Dataflow UI.

#### 5. Fault Tolerance (Dead-Letter Pattern)

* Any parsing or logic error emits a structured failure record
* Failed files are **not dropped silently**

---

## Design Principles

* **Schema-aware**: output aligns exactly with BigQuery schemas
* **Fail-safe**: errors are captured, not swallowed
* **Reusable**: transforms are pipeline-agnostic
* **Production-ready**: supports DataflowRunner execution

---

## Best Practices Followed

* Explicit type casting for numeric fields
* UTC-based ingestion timestamps
* Clear separation of parsing vs orchestration
* Defensive programming against malformed JSON

---

## Future Improvements

* Unit tests for each `DoFn`
* Field-level data quality checks
* Pydantic-based schema validation
* Additional event enrichment (xG, zones, phases of play)

---

Part of the **Sports Data Pipeline** project.
