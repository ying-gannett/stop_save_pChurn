import datetime
import json
import os
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
        with open(HISTORY_FILE, 'r') as f:
            for line in f:
                if line.strip():
                    history.append(json.loads(line.strip()))
    return history

def save_history(record: dict):
    """Appends a new record to the JSONL history file."""
    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    with open(HISTORY_FILE, 'a') as f:
        f.write(json.dumps(record) + "\n")

def check_deviation(current_val: float, historical_avg: float, metric_name: str) -> tuple[bool, str]:
    """
    Checks if the current value deviates by more than the threshold from the historical average.
    Returns (passed_boolean, alert_message).
    """
    if historical_avg == 0:
        return True, ""  # Nothing to compare against

    deviation = abs(current_val - historical_avg) / historical_avg
    if deviation > DEVIATION_THRESHOLD:
        direction = "increased" if current_val > historical_avg else "decreased"
        pct_change = deviation * 100
        msg = f"⚠️ ALERT: {metric_name} {direction} by {pct_change:.1f}%! (Current: {current_val:.1f}, Avg: {historical_avg:.1f})"
        return False, msg
    
    return True, ""

def run_assessment(file_path: str, run_date: str, sql_file: str, target_table: str) -> None:
    """
    Loads the downloaded data, calculates metrics, checks for anomalies against history,
    and updates the history file.
    """
    print(f"\n--- Starting Data Quality Assessment for {run_date} ---")
    
    # 1. Load Data
    try:
        if file_path.endswith(".parquet"):
            df = pd.read_parquet(file_path)
        else:
            df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Error loading {file_path} for assessment: {e}")
        return

    current_row_count = len(df)
    current_null_pct = calculate_null_percentage(df)
    
    print(f"Current Row Count: {current_row_count}")
    print(f"Current Null Percentage: {current_null_pct:.2f}%")
    
    # 2. Load History and Calculate Averages
    history = load_history()
    
    if not history:
        print("No historical data found. Skipping deviation checks and seeding first record.")
        row_count_passed = True
        nulls_passed = True
    else:
        avg_row_count = sum(r['row_count'] for r in history) / len(history)
        avg_null_pct = sum(r['null_percentage'] for r in history) / len(history)
        
        # 3. Check Deviations
        row_count_passed, row_alert = check_deviation(current_row_count, avg_row_count, "Row Count")
        nulls_passed, null_alert = check_deviation(current_null_pct, avg_null_pct, "Null Percentage")
        
        if not row_count_passed:
            print(row_alert)
        else:
            print("✅ Row count is within expected bounds.")
            
        if not nulls_passed:
            print(null_alert)
        else:
            print("✅ Null percentage is within expected bounds.")

    # 4. Save to History
    record = {
        "execution_timestamp": datetime.datetime.now().isoformat(),
        "run_date": run_date,
        "target_table": target_table,
        "sql_file": os.path.basename(sql_file),
        "row_count": current_row_count,
        "null_percentage": current_null_pct,
        "row_count_passed": row_count_passed,
        "nulls_passed": nulls_passed
    }
    save_history(record)
    print("✅ Run metrics saved to history.")
