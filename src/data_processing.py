import datetime
import os
import re
from pathlib import Path
from typing import Mapping, Optional

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

try:
    from .workflow_config import WorkflowTables, intervention_inference_date_sql
except ImportError:  # Supports direct execution of the workflow runner.
    from workflow_config import WorkflowTables, intervention_inference_date_sql

_TABLE_TOKEN = re.compile(r"\{\{([a-z][a-z0-9_]*)\}\}")


def resolve_sunday(run_date_str: str) -> datetime.date:
    """Resolve any date to the Sunday that starts its BigQuery reporting week."""
    run_date = datetime.date.fromisoformat(run_date_str)
    days_since_sunday = (run_date.weekday() + 1) % 7
    return run_date - datetime.timedelta(days=days_since_sunday)


def get_latest_partition_date(
    client: bigquery.Client, table_id: str, partition_field: str
) -> Optional[datetime.date]:
    """Queries BigQuery to find the maximum partition date in the target table."""
    query = f"SELECT MAX({partition_field}) as max_date FROM `{table_id}`"
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        return results[0].max_date
    except NotFound:
        # A missing table has no baseline. Authentication and SQL errors must
        # still propagate so catch-up cannot silently use the wrong behavior.
        return None


def get_daily_dates_after(
    start_date: datetime.date,
    end_date: datetime.date,
) -> list[datetime.date]:
    """Return daily dates between start (exclusive) and end (inclusive)."""
    dates = []
    current = start_date + datetime.timedelta(days=1)
    while current <= end_date:
        dates.append(current)
        current += datetime.timedelta(days=1)
    return dates


def check_guardrail(client: bigquery.Client, target_date_str: str, guardrail_table: str):
    """Check source availability, unless no guardrail table was configured."""
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
        cnt = res[0]["cnt"]

        if cnt == 0:
            raise RuntimeError(
                f"❌ Error: Data for {target_date_str} is not available in {guardrail_table} yet."
            )
        else:
            print(f"✅ Data available! Found {cnt} rows for {target_date_str}.")
    except Exception as exc:
        raise RuntimeError(f"Failed during guardrail check: {exc}") from exc


def execute_bq_query(
    client: bigquery.Client,
    sql_file: str,
    target_table_id: str,
    partition_field: Optional[str],
    target_date_str: str,
):
    """Execute a SELECT query into a table or one date partition."""
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"❌ Error: SQL file {sql_file} not found.")

    with open(sql_file, "r") as f:
        sql_template = f.read()

    sql_query = sql_template.format(run_date=target_date_str)

    if partition_field:
        # Format partition decorator: YYYYMMDD
        partition_decorator = target_date_str.replace("-", "")
        destination = f"{target_table_id}${partition_decorator}"
        job_config = bigquery.QueryJobConfig(
            destination=destination,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            ),
        )
        print(f"Executing query and saving to BigQuery partition `{destination}`...")
    else:
        destination = target_table_id
        job_config = bigquery.QueryJobConfig(
            destination=destination,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        print(f"Executing query and saving to BigQuery table `{destination}`...")

    try:
        query_job = client.query(sql_query, job_config=job_config)
        query_job.result()
        print("✅ Table populated successfully in BigQuery.")
    except Exception as exc:
        raise RuntimeError(f"Failed executing BigQuery SQL: {exc}") from exc


def download_local_cache(
    client: bigquery.Client,
    target_table_id: str,
    partition_field: Optional[str],
    target_date_str: str,
    local_output: Optional[str],
) -> tuple[str, pd.DataFrame]:
    """Download the target partition, or the whole table when unpartitioned."""
    if local_output is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        local_output = f"data/stop_save_source_{timestamp}.parquet"

    output_directory = os.path.dirname(local_output)
    if output_directory:
        os.makedirs(output_directory, exist_ok=True)
    print(f"Downloading data locally to {local_output}...")

    if partition_field:
        download_query = (
            f"SELECT * FROM `{target_table_id}` WHERE {partition_field} = DATE('{target_date_str}')"
        )
    else:
        download_query = f"SELECT * FROM `{target_table_id}`"

    try:
        df = client.query(download_query).to_dataframe()
        if local_output.endswith(".parquet"):
            df.to_parquet(local_output, index=False)
        else:
            df.to_csv(local_output, index=False)
        print(f"✅ Successfully downloaded {len(df)} rows to local cache.")
    except Exception as exc:
        raise RuntimeError(f"Failed downloading local copy: {exc}") from exc

    return local_output, df


def run_partition_query(
    client: bigquery.Client,
    target_date: datetime.date,
    target_table_id: str,
    partition_field: Optional[str],
    sql_file: str,
    guardrail_table: str,
    download: bool = False,
    local_output: Optional[str] = None,
) -> tuple[Optional[str], Optional[pd.DataFrame]]:
    """Populate one partition and optionally return the downloaded cache and DataFrame."""
    target_date_str = target_date.isoformat()

    check_guardrail(client, target_date_str, guardrail_table)
    execute_bq_query(client, sql_file, target_table_id, partition_field, target_date_str)

    if download:
        return download_local_cache(
            client,
            target_table_id,
            partition_field,
            target_date_str,
            local_output,
        )
    return None, None


def render_sql_template(sql_template: str, values: Mapping[str, str]) -> str:
    """Replace explicit ``{{name}}`` tokens and reject unknown tokens."""
    tokens = set(_TABLE_TOKEN.findall(sql_template))
    unknown = tokens.difference(values)
    if unknown:
        raise ValueError(f"Unknown SQL template token(s): {', '.join(sorted(unknown))}")

    rendered = sql_template
    for token in tokens:
        rendered = rendered.replace(f"{{{{{token}}}}}", values[token])

    unresolved = set(_TABLE_TOKEN.findall(rendered))
    if unresolved:
        raise ValueError(f"Unresolved SQL template token(s): {', '.join(sorted(unresolved))}")
    return rendered


def load_and_render_sql(
    sql_file: str | Path,
    tables: WorkflowTables,
    extra_values: Mapping[str, str] | None = None,
) -> str:
    """Load a workflow SQL file and resolve its environment-specific tokens."""
    path = Path(sql_file)
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {path}")
    values = tables.template_values()
    values["intervention_inference_date_sql"] = intervention_inference_date_sql()
    if extra_values:
        overlap = values.keys() & extra_values.keys()
        if overlap:
            raise ValueError(f"Template values cannot override: {', '.join(sorted(overlap))}")
        values.update(extra_values)
    return render_sql_template(path.read_text(), values)


def execute_sql_script(
    client: bigquery.Client,
    sql_file: str | Path,
    tables: WorkflowTables,
    extra_values: Mapping[str, str] | None = None,
) -> None:
    """Execute SQL that creates its own destination table or tables."""
    rendered_sql = load_and_render_sql(sql_file, tables, extra_values)
    print(f"Executing self-materializing BigQuery script `{sql_file}`...")
    query_job = client.query(rendered_sql)
    query_job.result()
    print(f"✅ Completed `{sql_file}` (job {query_job.job_id}).")
