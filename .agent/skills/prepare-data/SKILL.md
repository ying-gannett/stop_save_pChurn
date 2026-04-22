---
name: prepare-data
description: Data preparation and assessment pipeline for the pChurn Stop & Save project. Use this when the user asks to run the data pipeline, fetch churn data, or assess data quality.
---

# Prepare Data Skill

This skill orchestrates the data extraction and quality assessment pipeline for the pChurn Stop & Save project. The pipeline is designed to execute BigQuery SQL scripts to collect data, persist the results in BigQuery, and download local copies for analysis.

## Workflow

When asked to prepare data, run the data pipeline, or assess data quality, follow these steps strictly:

### 1. Execute the Data Pipeline

The data pipeline logic is handled by a Python script located at `src/data_processing.py` (or similar, depending on the current workspace state). 

- Execute the Python script to run the SQL files against BigQuery.
- **Example Execution:** `python src/data_processing.py` (adjust arguments as needed based on the current implementation of the script in the workspace).
- The Python script is responsible for determining SQL execution order based on dependencies.

### 2. Verify Output Files

After the pipeline runs successfully, verify that the local copies of the data have been generated in the `data/` directory.

- Check for the existence of the expected output files (e.g., CSV or Parquet files representing Churn Predictions, Intervention Data, Call Center Data, etc.).

### 3. Perform Data Quality Assessment

Once the local data is available, perform a preliminary data quality assessment using Python (e.g., via a pandas script or Jupyter notebook if requested).

Ensure you check for:
- **Availability:** Did the expected queries run and produce outputs? Specifically, ensure the Tuesday churn predictions exist.
- **Null Values:** Are there critical columns with high percentages of missing data?
- **Row Counts:** Do the row counts seem reasonable for the specific tables (e.g., matching expected weekly volumes)?
- **Schema Validation:** Do the output columns match the expected schemas for the retention analysis?

### Notes

- The actual SQL files are maintained in the project workspace (e.g., `src/sql/` or similar) and are tracked by Git.
- Do NOT attempt to run the SQL files directly using the `bq` CLI tool unless the Python orchestrator script is failing and you are attempting to debug a specific query. Always prefer the Python execution path.
