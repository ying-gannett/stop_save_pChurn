import argparse
import datetime
from data_processing import run_extraction
from data_assessment import run_assessment

def main():
    parser = argparse.ArgumentParser(description="Run Stop Save Data Pipeline and Quality Assessment")
    parser.add_argument("--run-date", type=str, help="Run date in YYYY-MM-DD format. Defaults to today.", default=datetime.date.today().isoformat())
    parser.add_argument("--project", type=str, default="gannett-datascience", help="BigQuery project ID")
    parser.add_argument("--dataset", type=str, default="test_activation_zone", help="BigQuery dataset name")
    parser.add_argument("--table", type=str, default="stop_save_test_Bart", help="BigQuery table name")
    parser.add_argument("--partition-field", type=str, default="inference_date")
    parser.add_argument("--sql-file", type=str, default="src/sql/stop_save_source.sql")
    parser.add_argument("--local-output", type=str, default=None, help="Path to save the local cache. If not provided, a default name with a timestamp is used.")
    
    args = parser.parse_args()
    
    # 1. Extraction Phase
    try:
        local_output_path, target_sunday_str, target_table_id = run_extraction(
            run_date_str=args.run_date,
            project=args.project,
            dataset=args.dataset,
            table=args.table,
            partition_field=args.partition_field,
            sql_file=args.sql_file,
            local_output=args.local_output
        )
    except Exception as e:
        print(f"\n❌ Pipeline failed during extraction: {e}")
        return

    # 2. Assessment Phase
    run_assessment(
        file_path=local_output_path,
        run_date=target_sunday_str,
        sql_file=args.sql_file,
        target_table=target_table_id
    )

if __name__ == "__main__":
    main()
