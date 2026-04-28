---
name: prepare-data
description: Data preparation and assessment pipeline for the pChurn Stop & Save project. Use this when the user asks to run the data pipeline, fetch churn data, or assess data quality for GA4 or Churn predictions.
---

# Prepare Data Skill

This skill orchestrates the data extraction and quality assessment pipeline for the pChurn Stop & Save project. The pipeline executes parameterized BigQuery SQL scripts, persists the results in BigQuery partitions, and optionally downloads local copies for quality assessment.

## Workflow

When asked to prepare data, run the data pipeline, or assess data quality, follow these steps:

### 1. Identify the Pipeline Configuration
Determine which SQL file and configuration to use based on the user's request:
- **Churn Predictions:** Use `stop_save_source.sql` (default). Usually requires `sunday` date-mode and data assessment.
- **Online Cancel (GA4):** Use `raw_online_cancel.sql`. Usually requires `exact` date-mode and `--skip-download`.

### 2. Execute the Data Pipeline
Execute the orchestrator using `uv run python src/run_pipeline.py`. 

**Parameters (Grouped by Function):**

*Core Source & Destination:*
- `--sql-file`: Path to the SQL file. Default: `src/sql/stop_save_source.sql`.
- `--run-date`: The target date (YYYY-MM-DD). Default: Today.
- `--table`: Target table name. Default: `stop_save_test_Bart`.
- `--dataset`: Target dataset name. Default: `test_activation_zone`.

*Pipeline Behavior:*
- `--date-mode`: `sunday` (calculates previous Sunday) or `exact` (uses run-date). Default: `sunday`.
- `--partition-field`: The field used for BQ partitioning. Default: `inference_date`.
- `--guardrail-table`: Table to check for availability. Pass `""` to bypass.
- `--catch-up`: Automatically fill missing partitions between the last entry in BQ and the `--run-date`.

*Output & Assessment:*
- `--skip-download`: Pass this flag to skip local download and bypass data assessment.
- `--local-output`: Path for local cache. Default: `data/stop_save_source_YYYYMMDD_HHMMSS.parquet`.

### 3. Monitor for Guardrails & Alerts (CRITICAL)
1. **Missing Data:** If the script fails because data is not available in the `guardrail-table`, report this to the user immediately.
2. **Data Anomalies:** If the pipeline outputs a `⚠️ ALERT` (e.g., >10% deviation in row counts or nulls), you **MUST HALT** immediately. Present the alert to the user and wait for approval before any further analysis.

## Execution Examples

**Example A: Standard Churn Pipeline**
`uv run python src/run_pipeline.py --run-date 2026-04-01 --table churn_results`

**Example B: GA4 Online Cancel Pipeline**
`uv run python src/run_pipeline.py --sql-file src/sql/raw_online_cancel.sql --table online_cancel_raw --partition-field event_date --date-mode exact --guardrail-table "" --skip-download`

**Example C: Catch-up Pipeline (Fill missing daily GA4 data)**
`uv run python src/run_pipeline.py --sql-file src/sql/raw_online_cancel.sql --table online_cancel_raw --partition-field event_date --date-mode exact --guardrail-table "" --skip-download --catch-up`

## Notes
- Do NOT run SQL directly via `bq` CLI; always use the Python orchestrator.
- Local parquet files are timestamped to prevent accidental overwrites.
