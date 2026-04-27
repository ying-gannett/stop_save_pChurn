import datetime
import os
from google.cloud import bigquery
from typing import Optional

def calculate_target_date(run_date_str: str, mode: str) -> datetime.date:
    """Calculates the target date based on the mode ('exact' or 'sunday')."""
    run_date = datetime.date.fromisoformat(run_date_str)
    if mode.lower() == 'exact':
        return run_date
    elif mode.lower() == 'sunday':
        idx = (run_date.weekday() + 1) % 7
        return run_date - datetime.timedelta(days=idx)
    else:
        raise ValueError(f"Unknown date-mode: {mode}. Use 'exact' or 'sunday'.")

def check_guardrail(client: bigquery.Client, target_date_str: str, guardrail_table: str):
    """Checks if the target date exists in the guardrail table. Skips if guardrail_table is empty."""
    if not guardrail_table:
        print("No guardrail table specified. Skipping availability check.")
        return

    print(f"Checking availability of data in `{guardrail_table}`...")
    guardrail_query = f"""
        SELECT count(*) as cnt
        FROM `{guardrail_table}`
        WHERE inference_date = DATE('{target_date_str}')
    """
    try:
        guardrail_job = client.query(guardrail_query)
        res = list(guardrail_job.result())
        cnt = res[0]['cnt']
        
        if cnt == 0:
            raise RuntimeError(f"❌ Error: Data for {target_date_str} is not available in {guardrail_table} yet.")
        else:
            print(f"✅ Data available! Found {cnt} rows for {target_date_str}.")
    except Exception as e:
        raise RuntimeError(f"Failed during guardrail check: {e}")

def execute_bq_query(client: bigquery.Client, sql_file: str, target_table_id: str, partition_field: str, target_date_str: str):
    """Reads SQL, applies partition decorator, and executes WRITE_TRUNCATE job."""
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"❌ Error: SQL file {sql_file} not found.")
        
    with open(sql_file, 'r') as f:
        sql_template = f.read()
        
    sql_query = sql_template.format(run_date=target_date_str)
    
    # Format partition decorator: YYYYMMDD (remove hyphens from ISO string)
    partition_decorator = target_date_str.replace("-", "")
    destination_partition = f"{target_table_id}${partition_decorator}"

    job_config = bigquery.QueryJobConfig(
        destination=destination_partition,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
    )
    
    print(f"Executing query and saving to BigQuery partition `{destination_partition}`...")
    try:
        query_job = client.query(sql_query, job_config=job_config)
        query_job.result()
        print("✅ Table populated successfully in BigQuery.")
    except Exception as e:
        raise RuntimeError(f"Failed executing BigQuery SQL: {e}")

def download_local_cache(client: bigquery.Client, target_table_id: str, partition_field: str, target_date_str: str, local_output: Optional[str]) -> str:
    """Downloads the target partition data to a local file."""
    if local_output is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        local_output = f"data/stop_save_source_{timestamp}.parquet"
        
    os.makedirs(os.path.dirname(local_output), exist_ok=True)
    print(f"Downloading data locally to {local_output}...")
    
    download_query = f"SELECT * FROM `{target_table_id}` WHERE {partition_field} = DATE('{target_date_str}')"
    
    try:
        df = client.query(download_query).to_dataframe()
        if local_output.endswith(".parquet"):
            df.to_parquet(local_output, index=False)
        else:
            df.to_csv(local_output, index=False)
        print(f"✅ Successfully downloaded {len(df)} rows to local cache.")
    except Exception as e:
        raise RuntimeError(f"Failed downloading local copy: {e}")
        
    return local_output

def run_extraction(run_date_str: str, project: str, dataset: str, table: str, partition_field: str, sql_file: str, local_output: Optional[str], date_mode: str, guardrail_table: str, skip_download: bool) -> tuple[Optional[str], str, str]:
    """
    Main orchestrator for the extraction phase.
    Returns (local_output_path, target_date_str, target_table_id).
    """
    client = bigquery.Client(project=project)
    target_table_id = f"{project}.{dataset}.{table}"
    
    # 1. Calculate Date
    target_date = calculate_target_date(run_date_str, date_mode)
    target_date_str = target_date.isoformat()
    print(f"Targeting pipeline execution for date: {target_date_str}")
    
    # 2. Check Guardrail
    check_guardrail(client, target_date_str, guardrail_table)
    
    # 3. Execute Query
    execute_bq_query(client, sql_file, target_table_id, partition_field, target_date_str)
    
    # 4. Download Cache (Conditional)
    downloaded_path = None
    if not skip_download:
        downloaded_path = download_local_cache(client, target_table_id, partition_field, target_date_str, local_output)
    else:
        print("Skipping local download as --skip-download was provided.")
        
    return downloaded_path, target_date_str, target_table_id
