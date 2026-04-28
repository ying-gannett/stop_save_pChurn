import argparse
import datetime
from google.cloud import bigquery
from data_processing import run_extraction, calculate_target_date, get_latest_partition_date, get_date_range
from data_assessment import run_assessment, log_run

def main():
    parser = argparse.ArgumentParser(description="Run Stop Save Data Pipeline and Quality Assessment")
    
    # 1. CORE SOURCE & DESTINATION
    parser.add_argument("--sql-file", type=str, default="src/sql/stop_save_source.sql", help="Path to parameterized SQL script")
    parser.add_argument("--run-date", type=str, help="Run date in YYYY-MM-DD format. Defaults to today.", default=datetime.date.today().isoformat())
    parser.add_argument("--table", type=str, default="stop_save_test_Bart", help="BigQuery target table name")
    parser.add_argument("--dataset", type=str, default="test_activation_zone", help="BigQuery dataset name")
    parser.add_argument("--project", type=str, default="gannett-datascience", help="BigQuery project ID")

    # 2. PIPELINE BEHAVIOR & LOGIC
    parser.add_argument("--date-mode", type=str, default="sunday", choices=["sunday", "exact"], help="How to calculate the target date. 'sunday' (default) uses previous Sunday, 'exact' uses --run-date.")
    parser.add_argument("--partition-field", type=str, default="inference_date", help="Field used for BigQuery time partitioning")
    parser.add_argument("--guardrail-table", type=str, default="gannett-enterprise-data.models_sz.pchurn_do_risk_tiers", help="Table to check for data availability. Pass empty string '' to bypass.")
    parser.add_argument("--catch-up", action="store_true", help="Automatically fill missing partitions between the last entry in BQ and the --run-date.")

    # 3. OUTPUT & ASSESSMENT
    parser.add_argument("--skip-download", action="store_true", help="Skip downloading local cache and bypass data assessment entirely.")
    parser.add_argument("--local-output", type=str, default=None, help="Path to save the local cache. If not provided, a timestamped parquet name is used.")
    
    args = parser.parse_args()
    
    client = bigquery.Client(project=args.project)
    target_table_id = f"{args.project}.{args.dataset}.{args.table}"
    
    # Determine the target date(s)
    end_date = calculate_target_date(args.run_date, args.date_mode)
    target_dates = [end_date]
    
    if args.catch_up:
        print(f"Checking for missing partitions in `{target_table_id}`...")
        latest_date = get_latest_partition_date(client, target_table_id, args.partition_field)
        
        if latest_date:
            print(f"Latest data found for: {latest_date}")
            missing_dates = get_date_range(latest_date, end_date, args.date_mode)
            if missing_dates:
                print(f"Identified {len(missing_dates)} missing dates to catch up.")
                target_dates = missing_dates
            else:
                print("No missing dates identified. Running only for the target date.")
        else:
            print("Target table empty or doesn't exist. Starting catch-up is not possible without a baseline. Running only for the target date.")

    # Execute the Pipeline for all identified dates
    for target_date in target_dates:
        print(f"\n--- Processing Date: {target_date.isoformat()} ---")
        try:
            local_output_path, target_date_str, target_table_id = run_extraction(
                client=client,
                target_date=target_date,
                project=args.project,
                dataset=args.dataset,
                table=args.table,
                partition_field=args.partition_field,
                sql_file=args.sql_file,
                local_output=args.local_output,
                guardrail_table=args.guardrail_table,
                skip_download=args.skip_download
            )
            
            # Assessment Phase
            if not args.skip_download and local_output_path:
                run_assessment(
                    file_path=local_output_path,
                    run_date=target_date_str,
                    sql_file=args.sql_file,
                    target_table=target_table_id
                )
            else:
                print("\nSkipping Data Assessment phase since local download was bypassed. Logging execution only.")
                log_run(
                    run_date=target_date_str,
                    sql_file=args.sql_file,
                    target_table=target_table_id
                )
        except Exception as e:
            print(f"❌ Failed processing {target_date.isoformat()}: {e}")
            if len(target_dates) > 1:
                print("Continuing with next date in range...")
            continue

    print("\n✅ Pipeline execution finished.")

if __name__ == "__main__":
    main()
