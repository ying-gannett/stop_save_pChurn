import argparse
import datetime
import os
import pandas as pd
from google.cloud import bigquery

def get_most_recent_sunday(date_obj: datetime.date) -> datetime.date:
    """Returns the Sunday of the week for the given date (assuming week starts on Sunday)."""
    # weekday() returns 0 for Monday, 6 for Sunday
    idx = (date_obj.weekday() + 1) % 7
    return date_obj - datetime.timedelta(days=idx)

def main():
    parser = argparse.ArgumentParser(description="Run Stop Save Data Pipeline")
    parser.add_argument("--run-date", type=str, help="Run date in YYYY-MM-DD format. Defaults to today.", default=datetime.date.today().isoformat())
    parser.add_argument("--project", type=str, default="gannett-datascience", help="BigQuery project ID")
    parser.add_argument("--dataset", type=str, default="test_activation_zone", help="BigQuery dataset name")
    parser.add_argument("--table", type=str, default="stop_save_test_Bart", help="BigQuery table name")
    parser.add_argument("--partition-field", type=str, default="inference_date")
    parser.add_argument("--sql-file", type=str, default="src/sql/stop_save_source.sql")
    parser.add_argument("--local-output", type=str, default="data/stop_save_source.parquet")
    
    args = parser.parse_args()
    
    # Construct full target table path
    target_table_id = f"{args.project}.{args.dataset}.{args.table}"
    
    # Initialize BigQuery Client
    client = bigquery.Client(project=args.project)
    
    run_date = datetime.date.fromisoformat(args.run_date)
    target_sunday = get_most_recent_sunday(run_date)
    
    print(f"Targeting prediction data for the week of Sunday: {target_sunday}")
    
    # --- Guardrail: Check if target_sunday exists in pchurn_do_risk_tiers ---
    print("Checking availability of churn predictions...")
    guardrail_query = f"""
        SELECT count(*) as cnt
        FROM `gannett-enterprise-data.models_sz.pchurn_do_risk_tiers`
        WHERE inference_date = DATE('{target_sunday}')
    """
    try:
        guardrail_job = client.query(guardrail_query)
        res = list(guardrail_job.result())
        cnt = res[0]['cnt']
        
        if cnt == 0:
            print(f"❌ Error: Churn predictions for {target_sunday} are not available yet.")
            return
        else:
            print(f"✅ Data available! Found {cnt} prediction rows for {target_sunday}.")
    except Exception as e:
        print(f"❌ Error querying guardrail table: {e}")
        return

    # --- Read and format SQL ---
    try:
        with open(args.sql_file, 'r') as f:
            sql_template = f.read()
    except FileNotFoundError:
        print(f"❌ Error: SQL file {args.sql_file} not found.")
        return
        
    # Format the query with the target_sunday
    sql_query = sql_template.format(run_date=target_sunday.isoformat())
    
    # --- Execute and Save to BQ ---
    # Configure the destination table
    job_config = bigquery.QueryJobConfig(
        destination=target_table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Overwrite target table
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=args.partition_field
        )
    )
    
    print(f"Executing query and saving to BigQuery table `{target_table_id}`...")
    try:
        query_job = client.query(sql_query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        print("✅ Table populated successfully in BigQuery.")
    except Exception as e:
        print(f"❌ Error executing BigQuery SQL: {e}")
        return
    
    # --- Download Local Copy ---
    os.makedirs(os.path.dirname(args.local_output), exist_ok=True)
    print(f"Downloading data locally to {args.local_output}...")
    
    # Select the data we just wrote to the target table
    download_query = f"SELECT * FROM `{target_table_id}` WHERE {args.partition_field} = DATE('{target_sunday}')"
    
    try:
        df = client.query(download_query).to_dataframe()
        
        if args.local_output.endswith(".parquet"):
            df.to_parquet(args.local_output, index=False)
        else:
            df.to_csv(args.local_output, index=False)
            
        print(f"✅ Successfully downloaded {len(df)} rows to local cache.")
    except Exception as e:
        print(f"❌ Error downloading local copy: {e}")

if __name__ == "__main__":
    main()
