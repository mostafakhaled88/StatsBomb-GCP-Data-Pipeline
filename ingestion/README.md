```
# Ingestion Layer: Sports Data Pipeline

This module serves as the **entry point (Extraction & Loading)** for the Sports Data Pipeline. It is responsible for programmatically fetching open-source event data from **StatsBomb** and landing it into **Google Cloud Storage (GCS)**.

This layer constitutes the **Bronze (Raw) Zone** of our data platform.

---

## 🏗 Core Responsibilities

* **E-L Pattern**: Extracts JSON from REST endpoints and Loads directly to Cloud Storage with minimal overhead.
* **Schema Neutrality**: Persists raw responses **unchanged** to prevent data loss and allow for future re-processing.
* **Hive Partitioning**: Organizes data using `key=value` directory structures, enabling **partition pruning** in BigQuery and Spark.
* **Traceability**: Embeds `ingestion_date` in every URI for auditing, point-in-time recovery, and historical snapshots.

> 💡 **Senior Note:** No transformation, validation, or schema enforcement occurs at this stage. We prioritize high-integrity data landing to ensure the raw source is always recoverable.

---

## 📁 Repository Layout

```text
ingestion/
├── config.py             # Environment constants, Bucket names, & API endpoints
├── gcs_client.py         # Thread-safe GCS storage wrapper
├── fetch_competitions.py # Level 1: Global league/season metadata
├── fetch_matches.py      # Level 2: Season/Match schedules
└── fetch_events.py       # Level 3: Multi-threaded play-by-play data fetcher

```

* * * * *

⚙️ Component Details
--------------------

### `config.py`

The "Source of Truth" for the environment. It abstracts GCP Project IDs and API timeouts. Moving these here ensures the code is **environment-agnostic** (easily portable from Dev to Prod).

### `gcs_client.py`

A lightweight wrapper for the `google-cloud-storage` SDK. It ensures consistent `Content-Type` headers (`application/json`) and manages the storage client lifecycle.

* * * * *

🛰 Data Fetchers & Partitioning Logic
-------------------------------------

### 1\. Competitions (`fetch_competitions.py`)

Fetches the global registry of available leagues and seasons. This is usually the first script to run.

-   **GCS Path:** `competitions/ingestion_date=YYYY-MM-DD/competitions.json`

### 2\. Matches (`fetch_matches.py`)

Iteratively fetches match schedules for every competition found in the metadata.

-   **GCS Path:** `matches/competition_id=XX/season_id=YY/ingestion_date=YYYY-MM-DD/matches.json`

### 3\. Events (`fetch_events.py`)

The high-volume ingestion engine. Since fetching thousands of match files is I/O intensive, this script utilizes **Python Concurrency (ThreadPoolExecutor)** to speed up the process by up to 10x.

-   **Input:** A CSV containing `competition_id`, `season_id`, and `match_id`.

-   **GCS Path:** `events/competition_id=XX/season_id=YY/match_id=ZZZ/ingestion_date=YYYY-MM-DD/events.json`

* * * * *

🛠 Operational Guide
--------------------

### 1\. Prerequisites

Bash

```
pip install google-cloud-storage requests
export GOOGLE_APPLICATION_CREDENTIALS="service-account-key.json"

```

### 2\. Execution Sequence

Bash

```
# Refresh Competition List
python ingestion/fetch_competitions.py

# Update Match Schedules
python ingestion/fetch_matches.py

# Batch Ingest Event Data (Parallelized with 10 workers)
python ingestion/fetch_events.py --matches_csv data/matches.csv --workers 10

```

* * * * *

✅ Engineering Design Principles
-------------------------------

-   **Idempotency:** Re-running a script on the same day overwrites the existing partition; running on a new day creates a new snapshot.

-   **Concurrency:** Utilizes multi-threading to overcome network latency during mass event ingestion.

-   **Fault Tolerance:** Individual HTTP 404/500 errors are caught and logged; the pipeline continues to process the remaining queue.

-   **Separation of Concerns:** Storage logic is decoupled from API request logic, allowing for easier testing and future migrations.