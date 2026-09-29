import datetime
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch

import pandas as pd

from src import data_assessment, run_prepare_data
from src.data_processing import load_and_render_sql, render_sql_template
from src.run_prepare_data import (
    GA_EARLIEST_DATE,
    determine_ga_target_dates,
    report_output_comparison,
    require_result_tables,
    require_usage_table,
    run_ga_stage,
    run_source_stage,
    run_workflow,
)
from src.workflow_config import (
    require_production_confirmation,
    resolve_workflow_tables,
)


class WorkflowConfigurationTests(unittest.TestCase):
    def test_staging_redirects_all_six_mutable_tables(self):
        tables = resolve_workflow_tables("staging")

        mutable_tables = tables.managed_outputs().values()
        self.assertEqual(len(tables.managed_outputs()), 6)
        self.assertTrue(all(".stop_save_refactor_staging." in table for table in mutable_tables))
        self.assertEqual(
            tables.intervention_table,
            "gannett-datascience.test_results_zone.stop_save_test_applied_Bart",
        )

    def test_production_requires_explicit_confirmation(self):
        with self.assertRaisesRegex(ValueError, "--confirm-production"):
            require_production_confirmation("production", False)

        require_production_confirmation("production", True)
        require_production_confirmation("staging", False)

    def test_staging_dataset_cannot_alias_a_production_dataset(self):
        with self.assertRaisesRegex(ValueError, "staging dataset must differ"):
            resolve_workflow_tables(
                "staging",
                staging_dataset="test_results_zone",
            )

    def test_self_materializing_queries_render_for_staging(self):
        tables = resolve_workflow_tables("staging")
        sql_files = (
            "src/sql/ss_test_result_P1_gcp_events.sql",
            "src/sql/ss_test_result_P2_gcp_sourced.sql",
            "src/sql/ss_test_result_P2_gcp_sourced_add_feas.sql",
        )

        for sql_file in sql_files:
            rendered = load_and_render_sql(sql_file, tables)
            self.assertNotIn("{{", rendered)
            self.assertIn("stop_save_refactor_staging", rendered)

        p2_sql = load_and_render_sql(sql_files[1], tables)
        self.assertIn(tables.intervention_table, p2_sql)
        self.assertIn(tables.stop_save_source_table, p2_sql)
        self.assertIn(tables.p1_event_table, p2_sql)

    def test_unknown_sql_template_token_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown_table"):
            render_sql_template(
                "SELECT * FROM `{{unknown_table}}`",
                {"known_table": "project.dataset.table"},
            )

    def test_output_comparison_reports_schema_drift_without_raising(self):
        class Field:
            def __init__(self, name):
                self.name = name
                self.field_type = "STRING"
                self.mode = "NULLABLE"

        class Table:
            def __init__(self, fields):
                self.schema = [Field(name) for name in fields]
                self.num_rows = 10

        staging = resolve_workflow_tables("staging")
        production = resolve_workflow_tables("production")
        metadata = {}
        for name, table_id in staging.managed_outputs().items():
            fields = ["id", "new_field"] if name == "p2_combined" else ["id"]
            metadata[table_id] = Table(fields)
        for table_id in production.managed_outputs().values():
            metadata[table_id] = Table(["id"])

        class Client:
            def get_table(self, table_id):
                return metadata[table_id]

        report_output_comparison(Client(), staging, production)

    def test_sql_file_must_exist(self):
        tables = resolve_workflow_tables("staging")
        with tempfile.TemporaryDirectory() as temporary_directory:
            missing = Path(temporary_directory) / "missing.sql"
            with self.assertRaises(FileNotFoundError):
                load_and_render_sql(missing, tables)


class GaCatchUpDateTests(unittest.TestCase):
    def test_existing_baseline_returns_dates_after_latest(self):
        dates = determine_ga_target_dates(
            latest_date=datetime.date(2026, 9, 4),
            end_date=datetime.date(2026, 9, 6),
        )
        self.assertEqual(
            dates,
            [datetime.date(2026, 9, 5), datetime.date(2026, 9, 6)],
        )

    def test_current_baseline_does_not_rewrite_end_date(self):
        dates = determine_ga_target_dates(
            latest_date=datetime.date(2026, 9, 6),
            end_date=datetime.date(2026, 9, 6),
        )
        self.assertEqual(dates, [])

    def test_empty_table_requires_start_date(self):
        with self.assertRaisesRegex(ValueError, "--ga-start-date"):
            determine_ga_target_dates(
                latest_date=None,
                end_date=datetime.date(2026, 9, 6),
            )

    def test_initialization_never_precedes_ga_floor(self):
        dates = determine_ga_target_dates(
            latest_date=None,
            start_date=datetime.date(2025, 12, 1),
            end_date=datetime.date(2025, 12, 30),
        )
        self.assertEqual(dates[0], GA_EARLIEST_DATE)
        self.assertEqual(dates[-1], datetime.date(2025, 12, 30))


class DataAssessmentTests(unittest.TestCase):
    def test_staging_assessment_can_compare_with_production_history(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            history_file = directory / "pipeline_history.jsonl"
            data_file = directory / "current.csv"
            history_file.write_text(
                json.dumps(
                    {
                        "execution_timestamp": "2026-09-01T00:00:00",
                        "run_date": "2026-08-30",
                        "target_table": "project.production.stop_save_test_Bart",
                        "sql_file": "stop_save_source.sql",
                        "row_count": 100,
                        "null_percentage": 0.0,
                        "row_count_passed": True,
                        "nulls_passed": True,
                    }
                )
                + "\n"
            )
            pd.DataFrame({"value": range(105)}).to_csv(data_file, index=False)

            with patch.object(data_assessment, "HISTORY_FILE", str(history_file)):
                passed = data_assessment.run_assessment(
                    dataframe=pd.read_csv(data_file),
                    run_date="2026-09-06",
                    sql_file="src/sql/stop_save_source.sql",
                    target_table="project.staging.stop_save_test_Bart",
                    history_target_table="project.production.stop_save_test_Bart",
                )

            self.assertTrue(passed)
            records = [json.loads(line) for line in history_file.read_text().splitlines()]
            self.assertEqual(len(records), 2)
            self.assertEqual(
                records[-1]["target_table"],
                "project.staging.stop_save_test_Bart",
            )


class WorkflowOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.tables = resolve_workflow_tables("staging")
        self.production_tables = resolve_workflow_tables("production")

    def test_full_workflow_runs_stages_in_order(self):
        observed = []

        with (
            patch.object(
                run_prepare_data,
                "require_intervention_week",
                side_effect=lambda *_: observed.append("preflight"),
            ),
            patch.object(
                run_prepare_data,
                "run_source_stage",
                side_effect=lambda *_: observed.append("source"),
            ),
            patch.object(
                run_prepare_data,
                "run_ga_stage",
                side_effect=lambda *_: observed.append("ga"),
            ),
            patch.object(
                run_prepare_data,
                "run_p1_stage",
                side_effect=lambda *_: observed.append("p1"),
            ),
            patch.object(
                run_prepare_data,
                "run_p2_stage",
                side_effect=lambda *_: observed.append("p2"),
            ),
            patch.object(
                run_prepare_data,
                "run_feature_stage",
                side_effect=lambda *_: observed.append("features"),
            ),
            patch.object(
                run_prepare_data,
                "report_output_comparison",
                side_effect=lambda *_: observed.append("comparison"),
            ),
        ):
            run_workflow(
                client=object(),
                tables=self.tables,
                production_tables=self.production_tables,
                run_date="2026-09-20",
            )

        self.assertEqual(
            observed,
            ["preflight", "source", "ga", "p1", "p2", "features", "comparison"],
        )

    def test_full_workflow_stops_after_failed_stage(self):
        ga_stage = Mock()
        with (
            patch.object(run_prepare_data, "require_intervention_week"),
            patch.object(
                run_prepare_data,
                "run_source_stage",
                side_effect=RuntimeError("source failed"),
            ),
            patch.object(run_prepare_data, "run_ga_stage", ga_stage),
        ):
            with self.assertRaisesRegex(RuntimeError, "source failed"):
                run_workflow(
                    client=object(),
                    tables=self.tables,
                    production_tables=self.production_tables,
                    run_date="2026-09-20",
                )

        ga_stage.assert_not_called()

    def test_ga_stage_logs_each_successful_partition(self):
        log = Mock()
        with (
            patch.object(
                run_prepare_data,
                "get_latest_partition_date",
                return_value=datetime.date(2026, 9, 4),
            ),
            patch.object(run_prepare_data, "run_partition_query"),
            patch.object(run_prepare_data, "require_partition"),
            patch.object(run_prepare_data, "require_ga_coverage"),
            patch.object(run_prepare_data, "log_run", log),
        ):
            run_ga_stage(
                client=object(),
                tables=self.tables,
                inference_date=datetime.date(2026, 9, 6),
                end_date=datetime.date(2026, 9, 6),
            )

        self.assertEqual(
            log.call_args_list,
            [
                call(
                    run_date="2026-09-05",
                    sql_file="src/sql/raw_ga_platform.sql",
                    target_table=self.tables.ga_platform_table,
                ),
                call(
                    run_date="2026-09-06",
                    sql_file="src/sql/raw_ga_platform.sql",
                    target_table=self.tables.ga_platform_table,
                ),
            ],
        )

    def test_source_assessment_reuses_downloaded_dataframe(self):
        dataframe = pd.DataFrame({"value": [1, 2]})
        assessment = Mock(return_value=True)
        with (
            patch.object(
                run_prepare_data,
                "run_partition_query",
                return_value=("cache.parquet", dataframe),
            ),
            patch.object(run_prepare_data, "require_partition"),
            patch.object(run_prepare_data, "run_assessment", assessment),
        ):
            run_source_stage(
                client=object(),
                tables=self.tables,
                production_tables=self.production_tables,
                inference_date=datetime.date(2026, 9, 20),
            )

        self.assertIs(assessment.call_args.kwargs["dataframe"], dataframe)

    def test_p2_validation_requires_the_requested_week(self):
        class QueryJob:
            def result(self):
                return [
                    {
                        "unfiltered_count": 20,
                        "filtered_count": 10,
                        "target_unfiltered_count": 0,
                        "target_filtered_count": 0,
                        "max_date": datetime.date(2026, 9, 27),
                    }
                ]

        client = Mock()
        client.query.return_value = QueryJob()
        with self.assertRaisesRegex(RuntimeError, "do not contain"):
            require_result_tables(
                client,
                self.tables,
                datetime.date(2026, 9, 20),
            )

    def test_feature_validation_requires_the_requested_week(self):
        class QueryJob:
            def result(self):
                return [
                    {
                        "row_count": 10,
                        "target_count": 0,
                        "max_date": datetime.date(2026, 9, 27),
                    }
                ]

        client = Mock()
        client.query.return_value = QueryJob()
        with self.assertRaisesRegex(RuntimeError, "does not contain"):
            require_usage_table(
                client,
                self.tables.usage_analysis_table,
                datetime.date(2026, 9, 20),
            )


if __name__ == "__main__":
    unittest.main()
