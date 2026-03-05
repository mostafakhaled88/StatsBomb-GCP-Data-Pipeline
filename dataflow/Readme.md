# Dataflow Module

This folder contains the **data ingestion and transformation layer** of the Sports Data Pipeline project. Its main responsibility is to extract raw data, validate it against schemas, apply transformations, and prepare it for downstream storage and analytics.

---

## Folder Structure

```
dataflow/
├── pipelines/     # End-to-end data pipelines (entry points)
├── schemas/       # Data schemas for validation and standardization
├── transforms/    # Data cleaning and transformation logic
├── utils/         # Shared helper functions and utilities
├── __init__.py    # Marks dataflow as a Python package
```

---

## pipelines/

Contains pipeline scripts that orchestrate the full data flow.

Typical responsibilities:

* Reading raw data (APIs, files, cloud storage, etc.)
* Calling transformation functions
* Validating data against schemas
* Writing processed data to storage (e.g., GCS, BigQuery, database)

Each pipeline usually represents **one data source or workflow**.

---

## schemas/

Defines schemas used to validate and structure incoming data.

Examples:

* JSON schema definitions
* Python dictionaries describing expected fields and data types

Purpose:

* Ensure data consistency
* Catch malformed or missing fields early
* Make pipelines more robust and predictable

---

## transforms/

Contains pure transformation logic.

Examples:

* Field renaming and standardization
* Data type conversions
* Feature engineering
* Normalization and enrichment

Design principle:

> Transformation functions should be **stateless and reusable**, with no direct I/O.

---

## utils/

Shared helper functions used across pipelines and transforms.

Examples:

* Safe dictionary access
* Logging helpers
* Date/time parsing
* Cloud storage helpers

This helps avoid code duplication and keeps pipelines clean.

---

## Design Philosophy

* **Separation of concerns**: pipelines, schemas, transforms, and utilities are clearly separated
* **Reusability**: transformation and utility functions are reusable across pipelines
* **Scalability**: easy to add new data sources or pipelines
* **Testability**: logic is modular and easy to test

---

## How This Fits in the Project

The `dataflow` module is responsible for preparing clean, validated data that will be consumed by:

* Data warehouse layers (bronze / silver / gold)
* Analytics dashboards
* Machine learning or statistical models

---

## Example Usage

```bash
python dataflow/pipelines/events_pipeline.py
```

---

## Future Improvements

* Add unit tests for transforms and utils
* Introduce schema validation libraries (e.g., Pydantic, Marshmallow)
* Add logging and monitoring hooks
* Support parallel or scheduled execution (Airflow / Cloud Composer)

---

Maintained as part of the **Sports Data Pipeline** project.
