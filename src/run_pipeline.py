import argparse
import datetime
from data_processing import run_extraction
from data_assessment import run_assessment

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
    
    # 3. OUTPUT & ASSESSMENT
    parser.add_argument("--skip-download", action="store_true", help="Skip downloading local cache and bypass data assessment entirely.")
    parser.add_argument("--local-output", type=str, default=None, help="Path to save the local cache. If not provided, a timestamped parquet name is used.")
    
    args = parser.parse_args()
    
    # 1. Extraction Phase
    try:
        local_output_path, target_date_str, target_table_id = run_extraction(
            run_date_str=args.run_date,
            project=args.project,
            dataset=args.dataset,
            table=args.table,
            partition_field=args.partition_field,
            sql_file=args.sql_file,
            local_output=args.local_output,
            date_mode=args.date_mode,
            guardrail_table=args.guardrail_table,
            skip_download=args.skip_download
        )
    except Exception as e:
        print(f"\n❌ Pipeline failed during extraction: {e}")
        return

    # 2. Assessment Phase
    if not args.skip_download and local_output_path:
        run_assessment(
            file_path=local_output_path,
            run_date=target_date_str,
            sql_file=args.sql_file,
            target_table=target_table_id
        )
    else:
        print("\nSkipping Data Assessment phase since local download was bypassed.")

if __name__ == "__main__":
    main()
