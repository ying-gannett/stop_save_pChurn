#!/usr/bin/env python3
"""Refresh the stable CSV used by Excel Power Query from BigQuery.

Usage:
    # Compare BigQuery with the stable CSV; write nothing:
    uv run python src/refresh_price_point_raw_data.py --dry-run

    # Back up the stable CSV, then replace it with the BigQuery result:
    uv run python src/refresh_price_point_raw_data.py

After a real run, open each workbook in desktop Excel, select Data > Refresh All, wait
for the query and PivotTables to finish, and save. Python never modifies either workbook.

Options:
    --csv PATH       stable Power Query CSV to compare and replace
    --sql PATH       BigQuery SQL file
    --project ID     GCP project billed for the query
    --from-csv PATH  read new data from a CSV instead of BigQuery (offline testing)
"""

import argparse
import datetime as dt
import shutil
import sys
from pathlib import Path

import pandas as pd

TOTAL_COLUMNS = ("Record Count", "Total Paid")
SCRIPT_DIR = Path(__file__).resolve().parent
WORKBOOK_DIR = SCRIPT_DIR / "workbooks"
DEFAULT_CSV = WORKBOOK_DIR / "price_point_raw_data.csv"
DEFAULT_SQL = SCRIPT_DIR / "sql" / "price_point_raw_data.sql"


def fetch_bigquery(sql_path: Path, project: str) -> pd.DataFrame:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    job = client.query(sql_path.read_text())
    df = job.result().to_dataframe()
    gb = (job.total_bytes_processed or 0) / 1e9
    print(f"BigQuery job {job.job_id}: {len(df):,} rows, {gb:.2f} GB processed")
    return df


def validate_result(df: pd.DataFrame) -> pd.DataFrame:
    columns = list(df.columns)
    invalid = [column for column in columns if not isinstance(column, str) or not column]
    duplicates = sorted({column for column in columns if columns.count(column) > 1})
    missing = [column for column in TOTAL_COLUMNS if column not in columns]
    if invalid:
        sys.exit(f"SQL result contains unnamed columns: {invalid}")
    if duplicates:
        sys.exit(f"SQL result contains duplicate columns: {duplicates}")
    if missing:
        sys.exit(f"SQL result is missing required total columns: {missing}")
    return df


def dataframe_summary(df: pd.DataFrame) -> dict:
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "records": int(pd.to_numeric(df["Record Count"]).sum()),
        "paid": round(float(pd.to_numeric(df["Total Paid"]).sum()), 2),
    }


def compare(reference: dict, new: dict) -> None:
    print(f"{'':14}{'rows':>8}{'columns':>10}{'records':>10}{'total paid':>14}")
    for label, values in (("stable csv", reference), ("query result", new)):
        print(
            f"{label:14}{values['rows']:>8,}{values['columns']:>10,}"
            f"{values['records']:>10,}{values['paid']:>14,.2f}"
        )


def replace_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        df.to_csv(temporary, index=False)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--sql", type=Path, default=DEFAULT_SQL)
    parser.add_argument("--project", default="gannett-datascience")
    parser.add_argument("--from-csv", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    csv_path = args.csv.expanduser().resolve()
    if not csv_path.exists():
        sys.exit(f"Stable Power Query CSV not found: {csv_path}")

    current_data = validate_result(pd.read_csv(csv_path))
    new_data = validate_result(
        pd.read_csv(args.from_csv)
        if args.from_csv
        else fetch_bigquery(args.sql, args.project)
    )
    compare(dataframe_summary(current_data), dataframe_summary(new_data))
    if args.dry_run:
        print("Dry run: no files changed.")
        return

    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    backups = csv_path.parent / "backups"
    backups.mkdir(exist_ok=True)
    backup = backups / f"price_point_raw_data_{stamp}.csv"
    shutil.copy2(csv_path, backup)
    replace_csv(csv_path, new_data)

    print(f"Stable Power Query CSV updated: {csv_path}")
    print(f"Previous stable CSV backed up: {backup}")
    print("Next: refresh and save both workbooks in desktop Excel.")


if __name__ == "__main__":
    main()
