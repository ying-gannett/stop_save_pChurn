---
name: prepare-data
description: Data preparation and assessment pipeline for the pChurn Stop & Save project. Use this when the user asks to run the data pipeline, fetch churn data, or assess data quality.
---

# Prepare Data Skill

This skill orchestrates the data extraction and quality assessment pipeline for the pChurn Stop & Save project. The pipeline executes parameterized BigQuery SQL scripts to collect churn predictions and subscription data, persists the results in BigQuery, and downloads local copies for analysis.

## Workflow

When asked to prepare data, run the data pipeline, or assess data quality, follow these steps strictly:

### 1. Execute the Data Pipeline

The data pipeline logic is handled by a Python script located at `src/data_processing.py`. 

Execute this script using `uv run`. You must parse the user's request to pass the appropriate flags. If the user doesn't specify values, use the script's defaults.

**Available Parameters:**
- `--run-date`: The target date (YYYY-MM-DD). The script will automatically calculate the most recent Sunday prior to this date. Default: Today.
- `--project`: BigQuery project ID. Default: `gannett-datascience`.
- `--dataset`: BigQuery dataset name. Default: `test_activation_zone`.
- `--table`: BigQuery target table name. Default: `stop_save_test_Bart`.
- `--local-output`: Path to save the local cache. Default: `data/stop_save_source.parquet`.

**Execution Examples:**
- *Basic (defaults):* `uv run python src/data_processing.py`
- *Specific Date & Table:* `uv run python src/data_processing.py --run-date 2026-04-01 --table custom_table_name`

**Important:** The script contains a guardrail that will fail and stop execution if the Tuesday churn predictions for the calculated Sunday are not available in BigQuery. If you encounter this error, report it directly to the user.

### 2. Verify Output Files

After the pipeline runs successfully, verify that the local copy of the data has been generated.

- Use the file system to check for the existence of `data/stop_save_source.parquet` (or the path specified by `--local-output`).

### 3. Perform Data Quality Assessment

Once the local data is available, perform a preliminary data quality assessment using Python (e.g., via a pandas script).

Ensure you check and report on:
- **Availability:** Did the expected queries run and produce outputs?
- **Null Values:** Are there critical columns with high percentages of missing data?
- **Row Counts:** Do the row counts seem reasonable for the specific tables (e.g., matching expected weekly volumes of ~600k+ rows)?

### Notes

- The SQL file (`src/sql/stop_save_source.sql`) is parameterized and tracked by Git. 
- Do NOT attempt to run the SQL file directly using the `bq` CLI tool. Always use the Python execution path.
