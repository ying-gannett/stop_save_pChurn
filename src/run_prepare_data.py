"""Run the ordered weekly Stop & Save data-preparation workflow."""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path
from typing import Sequence

from google.cloud import bigquery

try:
    from .data_assessment import log_run, run_assessment
    from .data_processing import (
        execute_sql_script,
        get_daily_dates_after,
        get_latest_partition_date,
        resolve_sunday,
        run_partition_query,
    )
    from .workflow_config import (
        DEFAULT_ACTIVATION_DATASET,
        DEFAULT_PROJECT,
        DEFAULT_RESULTS_DATASET,
        DEFAULT_STAGING_DATASET,
        WorkflowTables,
        intervention_inference_date_sql,
        require_production_confirmation,
        resolve_workflow_tables,
    )
except ImportError:  # Supports `python src/run_prepare_data.py`.
    from data_assessment import log_run, run_assessment
    from data_processing import (
        execute_sql_script,
        get_daily_dates_after,
        get_latest_partition_date,
        resolve_sunday,
        run_partition_query,
    )
    from workflow_config import (
        DEFAULT_ACTIVATION_DATASET,
        DEFAULT_PROJECT,
        DEFAULT_RESULTS_DATASET,
        DEFAULT_STAGING_DATASET,
        WorkflowTables,
        intervention_inference_date_sql,
        require_production_confirmation,
        resolve_workflow_tables,
    )

STOP_SAVE_SQL = Path("src/sql/stop_save_source.sql")
GA_PLATFORM_SQL = Path("src/sql/raw_ga_platform.sql")
P1_SQL = Path("src/sql/ss_test_result_P1_gcp_events.sql")
P2_SQL = Path("src/sql/ss_test_result_P2_gcp_sourced.sql")
FEATURE_SQL = Path("src/sql/ss_test_result_P2_gcp_sourced_add_feas.sql")

PCHURN_GUARDRAIL = "gannett-enterprise-data.models_sz.pchurn_do_risk_tiers"
GA_EARLIEST_DATE = datetime.date(2025, 12, 29)


def _query_one(client: bigquery.Client, query: str):
    rows = list(client.query(query).result())
    if len(rows) != 1:
        raise RuntimeError(f"Validation query returned {len(rows)} rows; expected exactly one.")
    return rows[0]


def require_nonempty_table(client: bigquery.Client, table_id: str) -> int:
    row = _query_one(client, f"SELECT COUNT(*) AS row_count FROM `{table_id}`")
    row_count = row["row_count"]
    if row_count <= 0:
        raise RuntimeError(f"Validation failed: `{table_id}` is empty.")
    print(f"✅ `{table_id}` contains {row_count:,} rows.")
    return row_count


def require_partition(
    client: bigquery.Client,
    table_id: str,
    partition_field: str,
    target_date: datetime.date,
) -> int:
    row = _query_one(
        client,
        f"""
        SELECT COUNT(*) AS row_count
        FROM `{table_id}`
        WHERE {partition_field} = DATE '{target_date.isoformat()}'
        """,
    )
    row_count = row["row_count"]
    if row_count <= 0:
        raise RuntimeError(
            f"Validation failed: `{table_id}` has no {partition_field} partition "
            f"for {target_date.isoformat()}."
        )
    print(
        f"✅ `{table_id}` has {row_count:,} rows for {partition_field}={target_date.isoformat()}."
    )
    return row_count


def require_intervention_week(
    client: bigquery.Client,
    intervention_table: str,
    inference_date: datetime.date,
) -> None:
    """Verify P2's external intervention input contains the requested week."""
    inference_date_expression = intervention_inference_date_sql()
    row = _query_one(
        client,
        f"""
        SELECT COUNT(*) AS row_count
        FROM `{intervention_table}`
        WHERE {inference_date_expression} = DATE '{inference_date.isoformat()}'
        """,
    )
    if row["row_count"] <= 0:
        raise RuntimeError(
            f"Preflight failed: `{intervention_table}` has no assignments for "
            f"inference_date={inference_date.isoformat()}."
        )
    print(
        f"✅ Intervention input contains {row['row_count']:,} rows for "
        f"inference_date={inference_date.isoformat()}."
    )


def determine_ga_target_dates(
    latest_date: datetime.date | None,
    end_date: datetime.date,
    start_date: datetime.date | None = None,
) -> list[datetime.date]:
    """Return daily GA dates to run, including both ends for a new baseline."""
    if end_date < GA_EARLIEST_DATE:
        raise ValueError(f"GA end date cannot be earlier than {GA_EARLIEST_DATE.isoformat()}.")

    if latest_date is not None:
        return get_daily_dates_after(latest_date, end_date)

    if start_date is None:
        raise ValueError(
            "The GA platform table has no baseline. Supply --ga-start-date to initialize it."
        )
    start_date = max(start_date, GA_EARLIEST_DATE)
    if start_date > end_date:
        raise ValueError("--ga-start-date must be on or before --ga-end-date.")
    return [
        start_date + datetime.timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    ]


def require_ga_coverage(
    client: bigquery.Client,
    table_id: str,
    inference_date: datetime.date,
    end_date: datetime.date,
) -> None:
    """Require the 90-day GA feature window to contain every daily partition."""
    start_date = max(GA_EARLIEST_DATE, inference_date - datetime.timedelta(days=90))
    row = _query_one(
        client,
        f"""
        WITH expected AS (
          SELECT event_date
          FROM UNNEST(
            GENERATE_DATE_ARRAY(
              DATE '{start_date.isoformat()}', DATE '{end_date.isoformat()}'
            )
          ) AS event_date
        ),
        actual AS (
          SELECT DISTINCT event_date
          FROM `{table_id}`
          WHERE event_date BETWEEN DATE '{start_date.isoformat()}'
            AND DATE '{end_date.isoformat()}'
        )
        SELECT ARRAY_AGG(expected.event_date ORDER BY expected.event_date) AS missing_dates
        FROM expected
        LEFT JOIN actual USING (event_date)
        WHERE actual.event_date IS NULL
        """,
    )
    missing_dates = row["missing_dates"] or []
    if missing_dates:
        formatted = ", ".join(str(value) for value in missing_dates[:10])
        suffix = "..." if len(missing_dates) > 10 else ""
        raise RuntimeError(
            f"GA coverage validation failed for `{table_id}`. Missing dates: {formatted}{suffix}"
        )
    print(
        f"✅ GA coverage is complete from {start_date.isoformat()} through {end_date.isoformat()}."
    )


def require_result_tables(
    client: bigquery.Client,
    tables: WorkflowTables,
    inference_date: datetime.date,
) -> None:
    row = _query_one(
        client,
        f"""
        SELECT
          (SELECT COUNT(*) FROM `{tables.p2_unfiltered_table}`) AS unfiltered_count,
          (SELECT COUNT(*) FROM `{tables.p2_combined_table}`) AS filtered_count,
          (SELECT COUNTIF(inference_date = DATE '{inference_date.isoformat()}')
           FROM `{tables.p2_unfiltered_table}`) AS target_unfiltered_count,
          (SELECT COUNTIF(inference_date = DATE '{inference_date.isoformat()}')
           FROM `{tables.p2_combined_table}`) AS target_filtered_count,
          (SELECT MAX(inference_date) FROM `{tables.p2_combined_table}`) AS max_date
        """,
    )
    unfiltered_count = row["unfiltered_count"]
    filtered_count = row["filtered_count"]
    target_unfiltered_count = row["target_unfiltered_count"]
    target_filtered_count = row["target_filtered_count"]
    max_date = row["max_date"]
    if unfiltered_count <= 0 or filtered_count <= 0:
        raise RuntimeError("P2 validation failed: a result table is empty.")
    if filtered_count > unfiltered_count:
        raise RuntimeError("P2 validation failed: filtered rows exceed unfiltered rows.")
    if target_unfiltered_count <= 0 or target_filtered_count <= 0:
        raise RuntimeError(
            f"P2 validation failed: outputs do not contain inference_date={inference_date}."
        )
    print(
        f"✅ P2 outputs validated: {unfiltered_count:,} unfiltered, "
        f"{filtered_count:,} filtered, {target_filtered_count:,} requested-week rows, "
        f"latest inference_date={max_date}."
    )


def require_usage_table(
    client: bigquery.Client,
    table_id: str,
    inference_date: datetime.date,
) -> None:
    row = _query_one(
        client,
        f"""
        SELECT
          COUNT(*) AS row_count,
          COUNTIF(inference_date = DATE '{inference_date.isoformat()}') AS target_count,
          MAX(inference_date) AS max_date
        FROM `{table_id}`
        """,
    )
    if row["row_count"] <= 0:
        raise RuntimeError(f"Feature validation failed: `{table_id}` is empty.")
    if row["target_count"] <= 0:
        raise RuntimeError(
            f"Feature validation failed: output does not contain inference_date={inference_date}."
        )
    print(
        f"✅ Feature output validated: {row['row_count']:,} rows, "
        f"{row['target_count']:,} requested-week rows, "
        f"latest inference_date={row['max_date']}."
    )


def report_output_comparison(
    client: bigquery.Client,
    tables: WorkflowTables,
    production_tables: WorkflowTables,
) -> None:
    """Compare staged table interfaces and row counts with production baselines."""
    if tables.managed_outputs() == production_tables.managed_outputs():
        return

    schemas_match = True
    print("\n=== Staging-to-production interface comparison ===")
    for name, table_id in tables.managed_outputs().items():
        production_table_id = production_tables.managed_outputs()[name]
        staged = client.get_table(table_id)
        production = client.get_table(production_table_id)
        staged_fields = {field.name: (field.field_type, field.mode) for field in staged.schema}
        production_fields = {
            field.name: (field.field_type, field.mode) for field in production.schema
        }
        print(f"{name}: staging rows={staged.num_rows:,}; production rows={production.num_rows:,}")
        added = sorted(staged_fields.keys() - production_fields.keys())
        removed = sorted(production_fields.keys() - staged_fields.keys())
        changed = sorted(
            field_name
            for field_name in staged_fields.keys() & production_fields.keys()
            if staged_fields[field_name] != production_fields[field_name]
        )
        if added or removed or changed:
            schemas_match = False
            print(
                f"  schema differences: added={added or 'none'}, "
                f"removed={removed or 'none'}, changed={changed or 'none'}"
            )
            for field_name in changed:
                production_field = production_fields[field_name]
                staged_field = staged_fields[field_name]
                print(
                    f"    {field_name}: {production_field[0]}/{production_field[1]} -> "
                    f"{staged_field[0]}/{staged_field[1]}"
                )

    if schemas_match:
        print("✅ All six staging schemas match their production counterparts.")
    else:
        print("⚠️ Staging schema changes require review before a production run.")


def run_source_stage(
    client: bigquery.Client,
    tables: WorkflowTables,
    production_tables: WorkflowTables,
    inference_date: datetime.date,
    local_output: str | None = None,
) -> None:
    print("\n=== Stage 1/5: weekly pChurn source ===")
    local_path, dataframe = run_partition_query(
        client=client,
        target_date=inference_date,
        target_table_id=tables.stop_save_source_table,
        partition_field="inference_date",
        sql_file=str(STOP_SAVE_SQL),
        guardrail_table=PCHURN_GUARDRAIL,
        download=True,
        local_output=local_output,
    )
    require_partition(client, tables.stop_save_source_table, "inference_date", inference_date)
    if not local_path or dataframe is None:
        raise RuntimeError("Weekly source assessment requires a downloaded DataFrame.")
    assessment_passed = run_assessment(
        dataframe=dataframe,
        run_date=inference_date.isoformat(),
        sql_file=str(STOP_SAVE_SQL),
        target_table=tables.stop_save_source_table,
        history_target_table=production_tables.stop_save_source_table,
    )
    if not assessment_passed:
        raise RuntimeError("Weekly source data-quality assessment raised an alert.")


def run_ga_stage(
    client: bigquery.Client,
    tables: WorkflowTables,
    inference_date: datetime.date,
    end_date: datetime.date,
    start_date: datetime.date | None = None,
) -> None:
    print("\n=== Stage 2/5: daily GA platform catch-up ===")
    latest_date = get_latest_partition_date(client, tables.ga_platform_table, "event_date")
    target_dates = determine_ga_target_dates(latest_date, end_date, start_date)
    print(f"GA dates to process: {len(target_dates)}")
    if not target_dates:
        print("✅ GA platform table is already current; no partition was rewritten.")

    for target_date in target_dates:
        run_partition_query(
            client=client,
            target_date=target_date,
            target_table_id=tables.ga_platform_table,
            partition_field="event_date",
            sql_file=str(GA_PLATFORM_SQL),
            guardrail_table="",
        )
        require_partition(client, tables.ga_platform_table, "event_date", target_date)
        log_run(
            run_date=target_date.isoformat(),
            sql_file=str(GA_PLATFORM_SQL),
            target_table=tables.ga_platform_table,
        )
    require_ga_coverage(client, tables.ga_platform_table, inference_date, end_date)


def run_p1_stage(client: bigquery.Client, tables: WorkflowTables) -> None:
    print("\n=== Stage 3/5: P1 GCP events ===")
    execute_sql_script(client, P1_SQL, tables)
    require_nonempty_table(client, tables.p1_event_table)


def run_p2_stage(
    client: bigquery.Client,
    tables: WorkflowTables,
    inference_date: datetime.date,
) -> None:
    print("\n=== Stage 4/5: P2 sourced results ===")
    execute_sql_script(client, P2_SQL, tables)
    require_result_tables(client, tables, inference_date)


def run_feature_stage(
    client: bigquery.Client,
    tables: WorkflowTables,
    inference_date: datetime.date,
) -> None:
    print("\n=== Stage 5/5: usage features ===")
    execute_sql_script(client, FEATURE_SQL, tables)
    require_usage_table(client, tables.usage_analysis_table, inference_date)


def run_workflow(
    client: bigquery.Client,
    tables: WorkflowTables,
    production_tables: WorkflowTables,
    run_date: str,
    stage: str = "all",
    ga_end_date: str | None = None,
    ga_start_date: str | None = None,
    local_output: str | None = None,
) -> None:
    inference_date = resolve_sunday(run_date)
    ga_end = datetime.date.fromisoformat(ga_end_date) if ga_end_date else inference_date
    ga_start = datetime.date.fromisoformat(ga_start_date) if ga_start_date else None
    if stage in {"all", "ga"} and ga_end < inference_date:
        raise ValueError("--ga-end-date must cover the resolved weekly inference Sunday.")

    print(f"Resolved weekly inference date: {inference_date.isoformat()}")
    if stage in {"all", "ga"}:
        print(f"Resolved inclusive GA end date: {ga_end.isoformat()}")
    if stage in {"all", "p2"}:
        require_intervention_week(client, tables.intervention_table, inference_date)

    if stage in {"all", "source"}:
        run_source_stage(client, tables, production_tables, inference_date, local_output)
    if stage in {"all", "ga"}:
        run_ga_stage(client, tables, inference_date, ga_end, ga_start)
    if stage in {"all", "p1"}:
        run_p1_stage(client, tables)
    if stage in {"all", "p2"}:
        run_p2_stage(client, tables, inference_date)
    if stage in {"all", "features"}:
        run_feature_stage(client, tables, inference_date)

    if stage == "all":
        report_output_comparison(client, tables, production_tables)
        print("\n✅ All five preparation stages completed and passed validation.")
    else:
        print(f"\n✅ Preparation stage `{stage}` completed and passed validation.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the ordered weekly Stop & Save data-preparation workflow."
    )
    parser.add_argument(
        "--run-date",
        required=True,
        help="A date in the target week; it resolves to that week's Sunday.",
    )
    parser.add_argument(
        "--stage",
        choices=("all", "source", "ga", "p1", "p2", "features"),
        default="all",
        help="Run the full workflow or one stage. Defaults to all.",
    )
    parser.add_argument(
        "--ga-end-date",
        help="Inclusive daily GA end date. Defaults to the resolved Sunday.",
    )
    parser.add_argument(
        "--ga-start-date",
        help="Required only when initializing an empty GA platform table.",
    )
    parser.add_argument(
        "--environment",
        choices=("staging", "production"),
        default="staging",
        help="Destination environment. Defaults to staging.",
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--staging-dataset", default=DEFAULT_STAGING_DATASET)
    parser.add_argument("--activation-dataset", default=DEFAULT_ACTIVATION_DATASET)
    parser.add_argument("--results-dataset", default=DEFAULT_RESULTS_DATASET)
    parser.add_argument("--local-output", help="Optional weekly source cache path.")
    parser.add_argument(
        "--confirm-production",
        action="store_true",
        help="Required with --environment production.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        require_production_confirmation(args.environment, args.confirm_production)
        tables = resolve_workflow_tables(
            environment=args.environment,
            project=args.project,
            staging_dataset=args.staging_dataset,
            activation_dataset=args.activation_dataset,
            results_dataset=args.results_dataset,
        )
        production_tables = resolve_workflow_tables(
            environment="production",
            project=args.project,
            staging_dataset=args.staging_dataset,
            activation_dataset=args.activation_dataset,
            results_dataset=args.results_dataset,
        )
        print(f"Destination environment: {args.environment}")
        print(f"Weekly source destination: {tables.stop_save_source_table}")
        print(f"GA platform destination: {tables.ga_platform_table}")
        print(f"Result destination: {tables.usage_analysis_table}")
        client = bigquery.Client(project=args.project)
        run_workflow(
            client=client,
            tables=tables,
            production_tables=production_tables,
            run_date=args.run_date,
            stage=args.stage,
            ga_end_date=args.ga_end_date,
            ga_start_date=args.ga_start_date,
            local_output=args.local_output,
        )
    except Exception as exc:
        print(f"\n❌ Data-preparation workflow stopped: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
