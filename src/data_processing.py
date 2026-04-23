import datetime
import os
from google.cloud import bigquery

def get_most_recent_sunday(date_obj: datetime.date) -> datetime.date:
    """Returns the Sunday of the week for the given date (assuming week starts on Sunday)."""
    idx = (date_obj.weekday() + 1) % 7
    return date_obj - datetime.timedelta(days=idx)

def run_extraction(run_date_str: str, project: str, dataset: str, table: str, partition_field: str, sql_file: str, local_output: str) -> tuple[str, str, str]:
    """
    Executes the BigQuery pipeline and downloads the result.
    Returns a tuple of (local_output_path, target_sunday_str, target_table_id).
    """
    if local_output is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        local_output = f"data/stop_save_source_{timestamp}.parquet"
        
    target_table_id = f"{project}.{dataset}.{table}"
    client = bigquery.Client(project=project)
    
    run_date = datetime.date.fromisoformat(run_date_str)
    target_sunday = get_most_recent_sunday(run_date)
    target_sunday_str = target_sunday.isoformat()
    
    print(f"Targeting prediction data for the week of Sunday: {target_sunday_str}")
    
    # --- Guardrail: Check if target_sunday exists in pchurn_do_risk_tiers ---
    print("Checking availability of churn predictions...")
    guardrail_query = f"""
        SELECT count(*) as cnt
        FROM `gannett-enterprise-data.models_sz.pchurn_do_risk_tiers`
        WHERE inference_date = DATE('{target_sunday_str}')
    """
    try:
        guardrail_job = client.query(guardrail_query)
        res = list(guardrail_job.result())
        cnt = res[0]['cnt']
        
        if cnt == 0:
            raise RuntimeError(f"❌ Error: Churn predictions for {target_sunday_str} are not available yet.")
        else:
            print(f"✅ Data available! Found {cnt} prediction rows for {target_sunday_str}.")
    except Exception as e:
        raise RuntimeError(f"Failed during guardrail check: {e}")

    # --- Read and format SQL ---
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"❌ Error: SQL file {sql_file} not found.")
        
    with open(sql_file, 'r') as f:
        sql_template = f.read()
        
    sql_query = sql_template.format(run_date=target_sunday_str)
    
    # --- Execute and Save to BQ ---
    partition_decorator = target_sunday.strftime("%Y%m%d")
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
    
    # --- Download Local Copy ---
    os.makedirs(os.path.dirname(local_output), exist_ok=True)
    print(f"Downloading data locally to {local_output}...")
    
    download_query = f"SELECT * FROM `{target_table_id}` WHERE {partition_field} = DATE('{target_sunday_str}')"
    
    try:
        df = client.query(download_query).to_dataframe()
        
        if local_output.endswith(".parquet"):
            df.to_parquet(local_output, index=False)
        else:
            df.to_csv(local_output, index=False)
            
        print(f"✅ Successfully downloaded {len(df)} rows to local cache.")
    except Exception as e:
        raise RuntimeError(f"Failed downloading local copy: {e}")
        
    return local_output, target_sunday_str, target_table_id
