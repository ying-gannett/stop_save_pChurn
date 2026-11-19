#!/usr/bin/env python3
"""
Refresh the repository's price point analysis workbook from BigQuery, replacing the
manual Looker Studio export.

One-time setup (Terminal on your Mac):
    uv sync
    gcloud auth application-default login      # lets Python use your gcloud identity

Usage:
    # 1) Compare BigQuery with the reference workbook, change nothing:
    uv run python src/refresh_price_point_raw_data.py --dry-run

    # 2) Refresh the reference workbook, then copy it to SharePoint:
    uv run python src/refresh_price_point_raw_data.py

Options:
    --workbook PATH        reference workbook (default: src/workbooks/price point analysis_v2.xlsx)
    --sharepoint-dir PATH  destination folder (default: ~/ds_pchurn)
    --sql PATH             query file (default: src/sql/price_point_raw_data.sql)
    --project ID           GCP project billed for the query (default: gannett-datascience)
    --from-csv PATH        use a CSV instead of BigQuery (offline testing)

For a real refresh, close both workbook copies in Excel. A timestamped workbook backup
and CSV snapshot are saved under src/workbooks/backups first. The 11 pivot tables refresh
when the updated workbook is next opened. The SharePoint copy excludes the Findings tab.
"""
import argparse
import datetime as dt
import re
import shutil
import sys
import tempfile
from copy import copy
from pathlib import Path

import openpyxl
import pandas as pd

SHEET = "price point raw data"
TABLE = "price_point_raw_data"
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_WORKBOOK = SCRIPT_DIR / "workbooks" / "price point analysis_v2.xlsx"
DEFAULT_SQL = SCRIPT_DIR / "sql" / "price_point_raw_data.sql"
DEFAULT_SHAREPOINT_DIR = Path.home() / "ds_pchurn"
SHAREPOINT_EXCLUDED_SHEETS = ("Findings",)
DATA_COLS = [
    "grouped_start_price_group", "pricegroup", "Cohort", "Treatment", "start_price",
    "step_up_price", "stop_save_price", "src_risk_tier", "Status", "gamer_flag",
    "Record Count", "Total Paid", "cnt_payments", "Contact Status", "Gamer Status",
]
KEY_COLS = [c for c in DATA_COLS if c not in ("Record Count", "Total Paid")]


def fetch_bigquery(sql_path: Path, project: str) -> pd.DataFrame:
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    job = client.query(sql_path.read_text())
    df = job.result().to_dataframe()
    gb = (job.total_bytes_processed or 0) / 1e9
    print(f"BigQuery job {job.job_id}: {len(df):,} rows, {gb:.2f} GB processed")
    return df


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in DATA_COLS if c not in df.columns]
    if missing:
        sys.exit(f"Query result is missing columns: {missing}")
    df = df[DATA_COLS].copy()
    for c in ("start_price", "step_up_price", "stop_save_price", "Total Paid"):
        df[c] = pd.to_numeric(df[c]).astype(float).round(4)
    for c in ("Record Count", "cnt_payments"):
        df[c] = pd.to_numeric(df[c]).astype("int64")
    nulls = df[KEY_COLS].isna().sum()
    if nulls.any():
        print("WARNING: null values in dimension columns (left blank):\n" + nulls[nulls > 0].to_string())
    return df


def read_current(ws) -> pd.DataFrame:
    rows = list(ws.iter_rows(min_row=2, max_col=len(DATA_COLS), values_only=True))
    rows = [r for r in rows if any(v is not None for v in r)]
    return normalise(pd.DataFrame(rows, columns=DATA_COLS))


def compare(old: pd.DataFrame, new: pd.DataFrame) -> None:
    def totals(d):
        return len(d), int(d["Record Count"].sum()), round(float(d["Total Paid"].sum()), 2)
    o, n = totals(old), totals(new)
    print(f"{'':14}{'rows':>8}{'records':>10}{'total paid':>14}")
    print(f"{'reference':14}{o[0]:>8,}{o[1]:>10,}{o[2]:>14,.2f}")
    print(f"{'new data':14}{n[0]:>8,}{n[1]:>10,}{n[2]:>14,.2f}")
    m = old.merge(new, on=KEY_COLS, how="outer", suffixes=("_old", "_new"), indicator=True)
    changed = m[(m["_merge"] == "both") & (
        (m["Record Count_old"] != m["Record Count_new"]) |
        ((m["Total Paid_old"] - m["Total Paid_new"]).abs() > 0.005))]
    print(f"Row-level: {(m['_merge'] == 'right_only').sum():,} new, "
          f"{(m['_merge'] == 'left_only').sum():,} gone, {len(changed):,} with changed counts/paid, "
          f"{(m['_merge'] == 'both').sum() - len(changed):,} identical")


def write_workbook(path: Path, df: pd.DataFrame) -> None:
    wb = openpyxl.load_workbook(path)
    ws = wb[SHEET]
    table = ws.tables[TABLE]
    first_col, last_col = re.match(r"([A-Z]+)\d+:([A-Z]+)\d+", table.ref).groups()
    ncols = openpyxl.utils.column_index_from_string(last_col)

    # Row 2 is the template for each column's style.
    template = {c: ws.cell(row=2, column=c) for c in range(1, ncols + 1)}
    styles = {c: (copy(cell.font), copy(cell.fill), copy(cell.border), copy(cell.alignment),
                  cell.number_format, copy(cell.protection)) for c, cell in template.items()}

    old_last = ws.max_row
    for r in range(2, old_last + 1):
        for c in range(1, ncols + 1):
            ws.cell(row=r, column=c).value = None

    for i, rec in enumerate(df.itertuples(index=False), start=2):
        for c in range(1, ncols + 1):
            cell = ws.cell(row=i, column=c)
            v = rec[c - 1]
            cell.value = None if pd.isna(v) else (v.item() if hasattr(v, "item") else v)
            f, fl, b, a, nf, p = styles[c]
            cell.font, cell.fill, cell.border, cell.alignment, cell.number_format, cell.protection = f, fl, b, a, nf, p

    new_last = len(df) + 1
    for r in range(new_last + 1, old_last + 1):          # drop leftover rows' formatting
        for c in range(1, ncols + 1):
            ws.cell(row=r, column=c).style = "Normal"
    table.ref = f"{first_col}1:{last_col}{new_last}"
    if table.autoFilter is not None:
        table.autoFilter.ref = table.ref

    for sheet in wb.worksheets:                           # pivots re-read the table on open
        for pivot in getattr(sheet, "_pivots", []):
            pivot.cache.refreshOnLoad = True
    wb.calculation.fullCalcOnLoad = True
    wb.save(path)


def copy_to_sharepoint(workbook_path: Path, sharepoint_dir: Path) -> Path:
    destination = sharepoint_dir / workbook_path.name
    try:
        with tempfile.TemporaryDirectory(prefix="price-point-sharepoint-") as tmp_dir:
            staged_path = Path(tmp_dir) / workbook_path.name
            wb = openpyxl.load_workbook(workbook_path)
            for sheet_name in SHAREPOINT_EXCLUDED_SHEETS:
                if sheet_name in wb.sheetnames:
                    del wb[sheet_name]
            wb.save(staged_path)
            wb.close()
            shutil.copy2(staged_path, destination)
    except OSError as exc:
        sys.exit(
            f"Reference workbook was updated, but the SharePoint copy failed: {exc}"
        )
    return destination


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    ap.add_argument("--sharepoint-dir", type=Path, default=DEFAULT_SHAREPOINT_DIR)
    ap.add_argument(
        "--sql",
        type=Path,
        default=DEFAULT_SQL,
    )
    ap.add_argument("--project", default="gannett-datascience")
    ap.add_argument("--from-csv", type=Path)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    wb_path = a.workbook.expanduser().resolve()
    sharepoint_dir = a.sharepoint_dir.expanduser()
    if not wb_path.exists():
        sys.exit(f"Workbook not found: {wb_path}")
    lock = wb_path.with_name("~$" + wb_path.name)
    if lock.exists() and not a.dry_run:
        sys.exit("The reference workbook looks open in Excel. Close it and re-run.")
    if not a.dry_run:
        if not sharepoint_dir.is_dir():
            sys.exit(f"SharePoint folder not found: {sharepoint_dir}")
        sharepoint_lock = sharepoint_dir / ("~$" + wb_path.name)
        if sharepoint_lock.exists():
            sys.exit("The SharePoint workbook looks open in Excel. Close it and re-run.")

    new = normalise(pd.read_csv(a.from_csv) if a.from_csv else fetch_bigquery(a.sql, a.project))
    current = read_current(openpyxl.load_workbook(wb_path, read_only=True, data_only=True)[SHEET])
    compare(current, new)
    if a.dry_run:
        print("Dry run: workbook not changed.")
        return

    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    backups = wb_path.parent / "backups"
    backups.mkdir(exist_ok=True)
    shutil.copy2(wb_path, backups / f"{wb_path.stem}_{stamp}{wb_path.suffix}")
    new.to_csv(backups / f"price_point_raw_data_{stamp}.csv", index=False)

    write_workbook(wb_path, new)
    destination = copy_to_sharepoint(wb_path, sharepoint_dir)
    print(f"Updated '{SHEET}' with {len(new):,} rows.")
    print(f"Backup and CSV snapshot saved in {backups}")
    print(f"Updated workbook copied to {destination} without the Findings tab")


if __name__ == "__main__":
    main()
