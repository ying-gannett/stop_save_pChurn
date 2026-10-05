"""Environment-specific table configuration for the Stop & Save workflow."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

DEFAULT_PROJECT = "gannett-datascience"
DEFAULT_STAGING_DATASET = "stop_save_refactor_staging"
DEFAULT_ACTIVATION_DATASET = "test_activation_zone"
DEFAULT_RESULTS_DATASET = "test_results_zone"

INTERVENTION_DATE_OVERRIDES = {
    "2026-04-08": "2026-03-29",
    "2026-04-09": "2026-04-05",
    "2026-08-31": "2026-08-23",
    "2026-09-07": "2026-08-30",
}

_IDENTIFIER_COMPONENT = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class WorkflowTables:
    """Fully-qualified tables read or written by the managed workflow."""

    stop_save_source_table: str
    ga_platform_table: str
    p1_event_table: str
    p2_unfiltered_table: str
    p2_combined_table: str
    p2_revenue_detail_table: str
    p2_revenue_summary_table: str
    usage_analysis_table: str
    intervention_table: str
    selected_markets_table: str

    def template_values(self) -> dict[str, str]:
        return asdict(self)

    def managed_outputs(self) -> dict[str, str]:
        """Return the mutable outputs controlled by this workflow."""
        return {
            "stop_save_source": self.stop_save_source_table,
            "ga_platform": self.ga_platform_table,
            "p1_event": self.p1_event_table,
            "p2_unfiltered": self.p2_unfiltered_table,
            "p2_combined": self.p2_combined_table,
            "p2_revenue_detail": self.p2_revenue_detail_table,
            "p2_revenue_summary": self.p2_revenue_summary_table,
            "usage_analysis": self.usage_analysis_table,
        }


def _validate_component(value: str, label: str) -> str:
    if not _IDENTIFIER_COMPONENT.fullmatch(value):
        raise ValueError(
            f"Invalid {label} {value!r}; use only letters, numbers, underscores, and hyphens."
        )
    return value


def qualified_table(project: str, dataset: str, table: str) -> str:
    """Return a validated, fully-qualified BigQuery table identifier."""
    return ".".join(
        (
            _validate_component(project, "project"),
            _validate_component(dataset, "dataset"),
            _validate_component(table, "table"),
        )
    )


def intervention_inference_date_sql() -> str:
    """Return the canonical SQL expression mapping an intervention file date to its week."""
    date_value = "SAFE_CAST(filedate AS DATE)"
    overrides = "\n".join(
        f"  WHEN {date_value} = DATE '{file_date}' THEN DATE '{inference_date}'"
        for file_date, inference_date in INTERVENTION_DATE_OVERRIDES.items()
    )
    return f"CASE\n{overrides}\n  ELSE DATE_TRUNC({date_value}, WEEK(SUNDAY))\nEND"


def resolve_workflow_tables(
    environment: str,
    project: str = DEFAULT_PROJECT,
    staging_dataset: str = DEFAULT_STAGING_DATASET,
    activation_dataset: str = DEFAULT_ACTIVATION_DATASET,
    results_dataset: str = DEFAULT_RESULTS_DATASET,
) -> WorkflowTables:
    """Resolve all workflow-managed tables for staging or production.

    Staging redirects all mutable workflow tables to the staging dataset.
    External intervention and market-reference inputs remain read-only production
    sources in both environments.
    """
    if environment not in {"staging", "production"}:
        raise ValueError("environment must be 'staging' or 'production'")

    if environment == "staging" and staging_dataset in {
        activation_dataset,
        results_dataset,
    }:
        raise ValueError(
            "The staging dataset must differ from both production datasets. "
            "Use --environment production --confirm-production for production writes."
        )

    mutable_activation_dataset = staging_dataset if environment == "staging" else activation_dataset
    mutable_results_dataset = staging_dataset if environment == "staging" else results_dataset

    return WorkflowTables(
        stop_save_source_table=qualified_table(
            project, mutable_activation_dataset, "stop_save_test_Bart"
        ),
        ga_platform_table=qualified_table(
            project, mutable_activation_dataset, "ss_test_ga4_platform"
        ),
        p1_event_table=qualified_table(
            project, mutable_results_dataset, "ss_test_result_v3-0_gcp_event"
        ),
        p2_unfiltered_table=qualified_table(
            project,
            mutable_results_dataset,
            "ss_test_result_p1_p2_combined_unfiltered",
        ),
        p2_combined_table=qualified_table(
            project, mutable_results_dataset, "ss_test_result_p1_p2_combined"
        ),
        p2_revenue_detail_table=qualified_table(
            project, mutable_results_dataset, "ss_test_result_p2_revenue_detail"
        ),
        p2_revenue_summary_table=qualified_table(
            project, mutable_results_dataset, "ss_test_result_p2_revenue_summary"
        ),
        usage_analysis_table=qualified_table(
            project, mutable_results_dataset, "ss_test_result_usage_analysis"
        ),
        intervention_table=qualified_table(project, results_dataset, "stop_save_test_applied_Bart"),
        selected_markets_table=qualified_table(
            project,
            activation_dataset,
            "stop_save_selected_80_mkts_with_subid",
        ),
    )


def require_production_confirmation(environment: str, confirmed: bool) -> None:
    """Prevent accidental writes to the production workflow tables."""
    if environment == "production" and not confirmed:
        raise ValueError(
            "Production execution requires --confirm-production after explicit user approval."
        )
