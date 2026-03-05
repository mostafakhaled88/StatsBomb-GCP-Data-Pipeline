# Dataflow Utilities

This folder contains **shared utility helpers** used across Dataflow pipelines and transforms. These utilities encapsulate common logic such as safe JSON access, GCS path parsing, and pipeline configuration.

Utilities are intentionally:

* Lightweight
* Reusable
* Free of Apache Beam pipeline logic

---

## Overview

```
utils/
├── filename_parser.py
├── pipeline_options.py
├── safe_get.py
```

---

## filename_parser.py

### `parse_gcs_path(path: str) -> dict`

#### Purpose

Safely extracts **partition and metadata values** from StatsBomb GCS file paths using pre-compiled regular expressions.

This utility is critical for deriving contextual metadata (e.g. `match_id`, `ingestion_date`) without relying on file contents.

---

### Extracted Fields

* `competition_id`
* `season_id`
* `match_id`
* `ingestion_date` (YYYY-MM-DD)
* `is_valid` (boolean flag)

---

### Key Features

#### 1. Pre-compiled Regex Patterns

* Regex patterns are compiled **once per worker** for better performance
* Avoids repeated compilation during element processing

#### 2. Defensive Parsing

* Missing metadata does **not crash the pipeline**
* Logs a warning instead of raising an exception
* Adds an `is_valid` flag so callers can route bad records to a dead-letter sink

#### 3. Type Safety

* Numeric identifiers are cast to `int`
* Dates remain strings for BigQuery `DATE` compatibility

---

### Example GCS Path

```
gs://statsbomb-raw/events/competition_id=16/season_id=4/match_id=12345/ingestion_date=2024-01-01/events.json
```

---

## pipeline_options.py

### `get_pipeline_options(...)`

#### Purpose

Provides a **centralized factory** for Apache Beam `PipelineOptions` used across pipelines.

This ensures consistent configuration for:

* Runner selection
* GCP project and region
* Temporary and staging locations
* Worker serialization

---

### Configuration Highlights

* `save_main_session=True` ensures custom logic is picklable on Dataflow workers
* `setup_file=./setup.py` enables dependency packaging
* Supports both `DirectRunner` (local) and `DataflowRunner` (GCP)

---

### Why Centralize Pipeline Options?

* Avoids duplication across pipelines
* Prevents configuration drift
* Makes production deployments safer and repeatable

---

## safe_get.py

### `safe_get(dct, *keys)`

#### Purpose

Safely extracts **nested dictionary values** from JSON objects.

Returns `None` if:

* Any intermediate value is not a dictionary
* A key is missing at any depth

---

### Example

```python
safe_get(event, "team", "name")
```

Equivalent to:

```python
event.get("team", {}).get("name")
```

But safer and reusable.

---

## Design Principles

* **Fail-safe by default**: utilities never crash pipelines
* **Single responsibility**: each utility does one thing well
* **Performance-aware**: optimized for Dataflow worker execution
* **Pipeline-agnostic**: usable outside Beam if needed

---

## Future Enhancements

* Path parsing via structured partition objects
* Typed return models (e.g., dataclasses)
* Centralized logging utilities

---

Part of the **Sports Data Pipeline** project.
