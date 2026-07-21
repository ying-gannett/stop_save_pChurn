import unittest

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
import numpy as np
import pandas as pd

from src import eda_helpers


class EdaHelpersTests(unittest.TestCase):
    def setUp(self):
        records = []
        account_index = 0
        segment_values = [
            ("Called-In Cancel Flow", "Before", "Low"),
            ("Called-In Cancel Flow", "After", "Low"),
            ("Online Cancel Flow", "Before", "Medium"),
            ("Online Cancel Flow", "After", np.nan),
        ]
        for segment_index, (channel, timing, risk) in enumerate(segment_values):
            for outcome in eda_helpers.OUTCOMES:
                for within_group in range(10):
                    is_saved = outcome == eda_helpers.SAVED
                    records.append(
                        {
                            "billing_account": f"acct_{account_index:03d}",
                            "outcome": outcome,
                            "contact_channel_group": channel,
                            "contact_timing": timing,
                            "cohort": "Three-Offer Cohort",
                            "src_risk_tier": risk,
                            "Treatment": ["Control", "Midpoint", "Tiered"][
                                (within_group + int(is_saved)) % 3
                            ],
                            "frequency": (
                                20 + segment_index * 3 + within_group + 10 * is_saved
                            ),
                            "breadth": 2 + segment_index + within_group / 5 + is_saved,
                            "tenure": 900 + segment_index * 100 + within_group * 10,
                            "tt_cost": (
                                500 + segment_index * 20 + within_group - 50 * is_saved
                            ),
                            "constant_metric": 5.0,
                        }
                    )
                    account_index += 1

        self.data = pd.DataFrame.from_records(records)
        self.metrics = [
            "frequency",
            "breadth",
            "tenure",
            "tt_cost",
            "constant_metric",
        ]
        self.segment_fields = [
            "contact_channel_group",
            "contact_timing",
            "cohort",
            "src_risk_tier",
        ]
        self.reference = eda_helpers.fit_behavior_reference(
            self.data,
            self.metrics,
        )
        self.profiles = eda_helpers.build_behavior_profiles(
            self.data,
            metrics=self.metrics,
            segment_fields=["outcome", *self.segment_fields],
            min_n=10,
            reference=self.reference,
        )
        self.contrasts = eda_helpers.build_outcome_contrasts(
            self.profiles,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
        )

    def tearDown(self):
        plt.close("all")

    def test_distribution_summary_includes_quality_percentiles_and_outliers(self):
        summary = eda_helpers.build_distribution_summary(
            pd.DataFrame({"metric": [1.0, 1.0, 1.0, 100.0, np.nan]}),
            ["metric"],
        ).iloc[0]

        self.assertEqual(summary["row_count"], 5)
        self.assertEqual(summary["non_null_count"], 4)
        self.assertEqual(summary["null_count"], 1)
        self.assertEqual(summary["median"], summary["p50"])
        self.assertEqual(summary["iqr"], summary["p75"] - summary["p25"])
        self.assertEqual(summary["outlier_count"], 1)
        self.assertEqual(summary["outlier_pct"], 25.0)

    def test_segment_summary_counts_unique_accounts_and_retains_missing_values(self):
        duplicated = pd.concat(
            [self.data, self.data.iloc[[0]], self.data.iloc[[1]]],
            ignore_index=True,
        )
        duplicated.loc[len(duplicated) - 1, "src_risk_tier"] = np.nan
        duplicated.loc[len(duplicated) - 1, "billing_account"] = "missing-risk"

        summary = eda_helpers.build_segment_summary(
            duplicated,
            "src_risk_tier",
            self.metrics,
        )

        low_users = summary.loc[
            summary["src_risk_tier"].eq("Low"),
            "users",
        ].item()
        missing_users = summary.loc[
            summary["src_risk_tier"].isna(),
            "users",
        ].item()
        self.assertEqual(low_users, 40)
        self.assertEqual(missing_users, 21)

    def test_common_reference_clips_and_robustly_scales_metrics(self):
        transformed = eda_helpers.transform_behavior_metrics(
            self.data,
            self.metrics,
            self.reference,
        )

        self.assertEqual(self.reference["spreads"]["constant_metric"], 1.0)
        np.testing.assert_array_equal(transformed["constant_metric"], 0.0)
        self.assertAlmostEqual(transformed["frequency"].median(), 0.0)
        self.assertTrue(np.isfinite(transformed[self.metrics].to_numpy()).all())

    def test_metric_boxplot_views_use_outcome_colors_and_three_value_scales(self):
        axes = eda_helpers.plot_metric_boxplot_views(
            self.data,
            metrics=self.metrics,
            group_col="outcome",
            group_order=list(eda_helpers.OUTCOMES),
            show_points=False,
            show=False,
            save=False,
            close=False,
        )

        self.assertEqual(len(axes), 3)
        self.assertIn("Original Values", axes[0].get_title())
        self.assertIn("Clipped Values", axes[1].get_title())
        self.assertIn("Standardized Clipped Values", axes[2].get_title())
        legend_colors = [
            handle.get_facecolor()
            for handle in axes[2].get_legend().legend_handles
        ]
        for actual, expected in zip(
            legend_colors,
            [
                eda_helpers.OUTCOME_COLORS[eda_helpers.SAVED],
                eda_helpers.OUTCOME_COLORS[eda_helpers.STOPPED],
            ],
        ):
            np.testing.assert_allclose(actual, to_rgba(expected))

    def test_outcome_contrasts_reuse_the_matched_profile_results(self):
        self.assertEqual(len(self.profiles), 8)
        self.assertEqual(len(self.contrasts), 4)
        self.assertTrue(self.contrasts["n__saved"].eq(10).all())
        self.assertTrue(self.contrasts["n__stopped"].eq(10).all())
        self.assertTrue(self.contrasts["delta__frequency"].gt(0).all())
        self.assertTrue(self.contrasts["delta__tt_cost"].lt(0).all())
        self.assertTrue(self.contrasts["contrast_magnitude"].is_monotonic_decreasing)

        first_contrast = self.contrasts.iloc[0]
        saved_profile = self.profiles[self.profiles["outcome"].eq("Saved")]
        for field in self.segment_fields:
            value = first_contrast[field]
            saved_profile = saved_profile[
                saved_profile[field].isna()
                if pd.isna(value)
                else saved_profile[field].eq(value)
            ]
        self.assertEqual(
            first_contrast["median_saved__frequency"],
            saved_profile["median__frequency"].item(),
        )

        with self.assertRaisesRegex(ValueError, "No Saved and Stopped"):
            eda_helpers.build_outcome_contrasts(
                self.profiles[self.profiles["outcome"].eq("Saved")],
                metrics=self.metrics,
                segment_fields=self.segment_fields,
            )

    def test_profile_contrast_heatmaps_and_standardized_drilldown_render(self):
        profile_ax = eda_helpers.plot_profile_or_contrast_heatmap(
            self.profiles,
            metrics=self.metrics,
            score_type="profile",
            save=False,
            show=False,
            close=False,
        )
        contrast_ax = eda_helpers.plot_profile_or_contrast_heatmap(
            self.contrasts,
            metrics=self.metrics,
            score_type="contrast",
            title="Saved minus Stopped",
            save=False,
            show=False,
            close=False,
        )
        axes = eda_helpers.plot_behavior_contrasts_boxplots(
            self.data,
            self.contrasts,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            reference=self.reference,
            top_n=2,
            save=False,
            show=False,
            close=False,
        )

        self.assertIn("Behavior profiles", profile_ax.get_title())
        self.assertIn("Saved minus Stopped", contrast_ax.get_title())
        self.assertEqual(len(axes), 2)
        self.assertEqual(axes[0].get_ylim(), axes[1].get_ylim())

    def test_selected_segment_details_add_spread_and_bootstrap_uncertainty(self):
        details = eda_helpers.build_selected_segment_detail_table(
            self.data,
            self.contrasts,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            reference=self.reference,
            top_n=2,
            bootstrap_iterations=100,
            random_state=7,
        )
        axes = eda_helpers.plot_selected_segment_clipped_boxplot_grid(
            self.data,
            self.contrasts,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            reference=self.reference,
            top_n=2,
            save=False,
            show=False,
            close=False,
        )

        self.assertEqual(len(details), 2 * len(self.metrics))
        self.assertTrue(details["n__saved"].eq(10).all())
        self.assertTrue(details["n__stopped"].eq(10).all())
        self.assertTrue(details["clipped_iqr__saved"].ge(0).all())
        self.assertTrue(details["clipped_median_difference_ci_lower"].notna().all())
        self.assertEqual(len(axes), 2 * len(self.metrics))
        self.assertEqual(axes[0].get_ylim(), axes[len(self.metrics)].get_ylim())

    def test_selected_segments_are_drilled_down_in_fixed_treatment_order(self):
        treatment_contrasts = (
            eda_helpers.build_treatment_contrasts(
                self.data,
                self.contrasts,
                metrics=self.metrics,
                segment_fields=self.segment_fields,
                reference=self.reference,
                top_n=2,
                min_n=2,
            )
        )
        treatment_ax = eda_helpers.plot_profile_or_contrast_heatmap(
            treatment_contrasts,
            metrics=self.metrics,
            score_type="contrast",
            title="Treatment drill-down",
            save=False,
            show=False,
            close=False,
        )

        self.assertEqual(len(treatment_contrasts), 6)
        self.assertEqual(
            treatment_contrasts.groupby("segment_rank", sort=False)["Treatment"]
            .apply(list)
            .tolist(),
            [
                ["Control", "Midpoint", "Tiered"],
                ["Control", "Midpoint", "Tiered"],
            ],
        )
        self.assertTrue(treatment_contrasts["supported"].all())
        self.assertTrue(treatment_contrasts["delta__frequency"].notna().all())
        self.assertIn("Treatment drill-down", treatment_ax.get_title())

        axes = eda_helpers.plot_behavior_contrasts_boxplots(
            self.data,
            treatment_contrasts,
            metrics=self.metrics,
            segment_fields=[*self.segment_fields, "Treatment"],
            reference=self.reference,
            top_n=2,
            save=False,
            show=False,
            close=False,
        )
        self.assertEqual(len(axes), 2)
        for ax in axes:
            backgrounds = {
                patch.get_gid()
                for patch in ax.patches
                if patch.get_gid() is not None
            }
            self.assertEqual(
                backgrounds,
                {
                    "treatment-background-Control",
                    "treatment-background-Midpoint",
                    "treatment-background-Tiered",
                },
            )
            self.assertEqual(
                [tick.get_text() for tick in ax.get_xticklabels()],
                self.metrics * len(eda_helpers._TREATMENT_ORDER),
            )
            treatment_headers = " ".join(text.get_text() for text in ax.texts)
            for treatment in eda_helpers._TREATMENT_ORDER:
                self.assertIn(treatment, treatment_headers)

        metric_first_axes = eda_helpers.plot_behavior_contrasts_boxplots(
            self.data,
            treatment_contrasts,
            metrics=self.metrics,
            segment_fields=[*self.segment_fields, "Treatment"],
            reference=self.reference,
            top_n=2,
            treatment_layout="metric_first",
            save=False,
            show=False,
            close=False,
        )
        self.assertEqual(len(metric_first_axes), 2)
        for ax in metric_first_axes:
            backgrounds = [
                patch.get_gid()
                for patch in ax.patches
                if patch.get_gid() is not None
            ]
            self.assertEqual(
                len(backgrounds),
                len(self.metrics) * len(eda_helpers._TREATMENT_ORDER),
            )
            self.assertEqual(
                [tick.get_text() for tick in ax.get_xticklabels()],
                eda_helpers._TREATMENT_ORDER * len(self.metrics),
            )
            metric_headers = " ".join(text.get_text() for text in ax.texts)
            for metric in self.metrics:
                self.assertIn(metric, metric_headers)


if __name__ == "__main__":
    unittest.main()
