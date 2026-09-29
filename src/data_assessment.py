import datetime
import json
import os
from typing import Optional

import pandas as pd

HISTORY_FILE = ".agent/pipeline_history.jsonl"
DEVIATION_THRESHOLD = 0.10  # 10%


def calculate_null_percentage(df: pd.DataFrame) -> float:
    """Calculates the average percentage of nulls across all columns."""
    if df.empty:
        return 0.0
    null_counts = df.isnull().sum()
    total_cells = df.shape[0] * df.shape[1]
    total_nulls = null_counts.sum()
    return (total_nulls / total_cells) * 100


def load_history() -> list[dict]:
    """Loads the pipeline history from the JSONL file."""
    history = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            for line in f:
                if line.strip():
                    history.append(json.loads(line.strip()))
    return history


def log_run(
    run_date: str,
    sql_file: str,
    target_table: str,
    row_count: Optional[int] = None,
    null_percentage: Optional[float] = None,
    row_count_passed: Optional[bool] = None,
    nulls_passed: Optional[bool] = None,
):
    """Logs the execution metrics to the JSONL history file."""
    record = {
        "execution_timestamp": datetime.datetime.now().isoformat(),
        "run_date": run_date,
        "target_table": target_table,
        "sql_file": os.path.basename(sql_file),
        "row_count": row_count,
        "null_percentage": null_percentage,
        "row_count_passed": row_count_passed,
        "nulls_passed": nulls_passed,
    }
    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    with open(HISTORY_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


def check_deviation(
    current_val: float, historical_avg: float, metric_name: str
) -> tuple[bool, str]:
    """Checks if the current value deviates by more than the threshold."""
    if historical_avg == 0:
        return True, ""
    deviation = abs(current_val - historical_avg) / historical_avg
    if deviation > DEVIATION_THRESHOLD:
        direction = "increased" if current_val > historical_avg else "decreased"
        pct_change = deviation * 100
        msg = (
            f"⚠️ ALERT: {metric_name} {direction} by {pct_change:.1f}%! "
            f"(Current: {current_val:.1f}, Avg: {historical_avg:.1f})"
        )
        return False, msg
    return True, ""


def run_assessment(
    dataframe: pd.DataFrame,
    run_date: str,
    sql_file: str,
    target_table: str,
    history_target_table: Optional[str] = None,
) -> bool:
    """Assess a downloaded DataFrame, log it, and return whether checks passed.

    ``history_target_table`` lets a staging run compare against the matching
    production table without mixing staging metrics into future production
    comparisons.
    """
    print(f"\n--- Starting Data Quality Assessment for {run_date} ---")
    current_row_count = len(dataframe)
    current_null_pct = calculate_null_percentage(dataframe)

    print(f"Current Row Count: {current_row_count}")
    print(f"Current Null Percentage: {current_null_pct:.2f}%")

    current_sql_filename = os.path.basename(sql_file)
    comparison_table = history_target_table or target_table
    history = [
        record
        for record in load_history()
        if record.get("sql_file") == current_sql_filename
        and record.get("target_table") == comparison_table
        and record.get("row_count") is not None
        and record.get("null_percentage") is not None
    ]

    if not history:
        print("No historical metrics found for comparison for this SQL file. Seeding first record.")
        row_count_passed, nulls_passed = True, True
    else:
        avg_row_count = sum(record["row_count"] for record in history) / len(history)
        avg_null_pct = sum(record["null_percentage"] for record in history) / len(history)

        row_count_passed, row_alert = check_deviation(current_row_count, avg_row_count, "Row Count")
        nulls_passed, null_alert = check_deviation(
            current_null_pct, avg_null_pct, "Null Percentage"
        )

        if not row_count_passed:
            print(row_alert)
        else:
            print("✅ Row count is within expected bounds.")

        if not nulls_passed:
            print(null_alert)
        else:
            print("✅ Null percentage is within expected bounds.")

    log_run(
        run_date,
        sql_file,
        target_table,
        current_row_count,
        current_null_pct,
        row_count_passed,
        nulls_passed,
    )
    print("✅ Run metrics saved to history.")
    return row_count_passed and nulls_passed
