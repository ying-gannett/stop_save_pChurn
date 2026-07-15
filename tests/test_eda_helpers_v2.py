import unittest

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src import eda_helpers_v2


class BehaviorProfilingV2Tests(unittest.TestCase):
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
            for outcome in ["Saved", "Stopped"]:
                for within_group in range(10):
                    is_saved = outcome == "Saved"
                    records.append(
                        {
                            "billing_account": f"acct_{account_index:03d}",
                            "outcome": outcome,
                            "contact_channel_group": channel,
                            "contact_timing": timing,
                            "cohort": "Three-Offer Cohort",
                            "src_risk_tier": risk,
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
        self.reference = eda_helpers_v2.fit_behavior_reference(
            self.data,
            self.metrics,
        )

    def tearDown(self):
        plt.close("all")

    def test_common_reference_clips_and_robustly_scales_metrics(self):
        transformed = eda_helpers_v2.transform_behavior_metrics(
            self.data,
            self.metrics,
            self.reference,
        )

        self.assertEqual(self.reference["spreads"]["constant_metric"], 1.0)
        np.testing.assert_array_equal(transformed["constant_metric"], 0.0)
        self.assertAlmostEqual(transformed["frequency"].median(), 0.0)

    def test_profiles_use_unique_counts_and_retain_missing_segments(self):
        profiles = eda_helpers_v2.build_behavior_profiles(
            self.data,
            metrics=self.metrics,
            segment_fields=["outcome"],
            min_n=20,
            reference=self.reference,
        )

        self.assertEqual(set(profiles["outcome"]), {"Saved", "Stopped"})
        self.assertEqual(set(profiles["users"]), {40})
        saved = profiles.set_index("outcome").loc["Saved"]
        stopped = profiles.set_index("outcome").loc["Stopped"]
        self.assertGreater(saved["score__frequency"], stopped["score__frequency"])
        self.assertLess(saved["score__tt_cost"], stopped["score__tt_cost"])

        missing_profiles = eda_helpers_v2.build_behavior_profiles(
            self.data,
            metrics=self.metrics,
            segment_fields=["src_risk_tier"],
            min_n=1,
            reference=self.reference,
        )
        self.assertTrue(missing_profiles["src_risk_tier"].isna().any())
        self.assertTrue(
            missing_profiles.loc[
                missing_profiles["src_risk_tier"].isna(),
                "segment_label",
            ].eq("Missing").all()
        )
        with self.assertRaisesRegex(ValueError, "No behavior profiles"):
            eda_helpers_v2.build_behavior_profiles(
                self.data,
                metrics=self.metrics,
                segment_fields=["outcome"],
                min_n=41,
                reference=self.reference,
            )

    def test_outcome_contrasts_are_matched_and_ranked(self):
        contrasts = eda_helpers_v2.build_outcome_contrasts(
            self.data,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            outcome_col="outcome",
            outcomes=("Saved", "Stopped"),
            min_n=10,
            reference=self.reference,
        )

        self.assertEqual(len(contrasts), 4)
        self.assertTrue(contrasts["n__saved"].eq(10).all())
        self.assertTrue(contrasts["n__stopped"].eq(10).all())
        self.assertTrue(contrasts["delta__frequency"].gt(0).all())
        self.assertTrue(contrasts["delta__tt_cost"].lt(0).all())
        self.assertTrue(contrasts["observed_saved_share"].eq(0.5).all())
        self.assertTrue(contrasts["contrast_magnitude"].is_monotonic_decreasing)

        with self.assertRaisesRegex(ValueError, "No matched outcome contrasts"):
            eda_helpers_v2.build_outcome_contrasts(
                self.data,
                metrics=self.metrics,
                segment_fields=self.segment_fields,
                outcome_col="outcome",
                outcomes=("Saved", "Stopped"),
                min_n=11,
                reference=self.reference,
            )

    def test_heatmaps_and_selected_drilldowns_render(self):
        profiles = eda_helpers_v2.build_behavior_profiles(
            self.data,
            metrics=self.metrics,
            segment_fields=["outcome", *self.segment_fields],
            min_n=10,
            reference=self.reference,
        )
        contrasts = eda_helpers_v2.build_outcome_contrasts(
            self.data,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            outcome_col="outcome",
            outcomes=("Saved", "Stopped"),
            min_n=10,
            reference=self.reference,
        )

        profile_ax = eda_helpers_v2.plot_behavior_profile_heatmap(
            profiles,
            metrics=self.metrics,
            save=False,
            show=False,
            close=False,
        )
        contrast_ax = eda_helpers_v2.plot_outcome_contrast_heatmap(
            contrasts,
            metrics=self.metrics,
            outcomes=("Saved", "Stopped"),
            save=False,
            show=False,
            close=False,
        )
        axes = eda_helpers_v2.plot_top_behavior_contrasts(
            self.data,
            contrasts,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            reference=self.reference,
            outcome_col="outcome",
            outcomes=("Saved", "Stopped"),
            top_n=2,
            save=False,
            show=False,
            close=False,
        )

        self.assertIn("Behavior profiles", profile_ax.get_title())
        self.assertIn("Saved minus Stopped", contrast_ax.get_title())
        self.assertEqual(len(axes), 2)
        self.assertEqual(axes[0].get_ylim(), axes[1].get_ylim())

    def test_selected_detail_table_reuses_counts_and_adds_clipped_uncertainty(self):
        contrasts = eda_helpers_v2.build_outcome_contrasts(
            self.data,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            outcome_col="outcome",
            outcomes=("Saved", "Stopped"),
            min_n=10,
            reference=self.reference,
        )
        details = eda_helpers_v2.build_selected_segment_detail_table(
            self.data,
            contrasts,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            reference=self.reference,
            outcome_col="outcome",
            outcomes=("Saved", "Stopped"),
            top_n=2,
            bootstrap_iterations=200,
            random_state=7,
        )

        self.assertEqual(len(details), 10)
        self.assertEqual(set(details["segment_rank"]), {1, 2})
        self.assertTrue(details["n__saved"].eq(10).all())
        self.assertTrue(details["n__stopped"].eq(10).all())
        self.assertTrue(details["non_null__saved"].eq(10).all())
        self.assertTrue(details["non_null__stopped"].eq(10).all())
        self.assertTrue(details["clipped_iqr__saved"].ge(0).all())
        self.assertTrue(details["clipped_iqr__stopped"].ge(0).all())
        first_detail = details.iloc[0]
        first_contrast = contrasts.iloc[0]
        expected_saved_median = np.clip(
            first_contrast[f"median_saved__{first_detail['metric']}"],
            self.reference["lower_bounds"][first_detail["metric"]],
            self.reference["upper_bounds"][first_detail["metric"]],
        )
        self.assertEqual(
            first_detail["clipped_median__saved"],
            expected_saved_median,
        )
        self.assertTrue(
            details["clipped_median_difference_ci_lower"].notna().all()
        )
        self.assertTrue(
            details["clipped_median_difference_ci_lower"].le(
                details["clipped_median_difference_ci_upper"]
            ).all()
        )

    def test_selected_clipped_boxplot_grid_shares_axes_by_metric(self):
        contrasts = eda_helpers_v2.build_outcome_contrasts(
            self.data,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            outcome_col="outcome",
            outcomes=("Saved", "Stopped"),
            min_n=10,
            reference=self.reference,
        )
        clipped_axes = eda_helpers_v2.plot_selected_segment_clipped_boxplot_grid(
            self.data,
            contrasts,
            metrics=self.metrics,
            segment_fields=self.segment_fields,
            reference=self.reference,
            outcome_col="outcome",
            outcomes=("Saved", "Stopped"),
            top_n=2,
            save=False,
            show=False,
            close=False,
        )
        metric_count = len(self.metrics)
        self.assertEqual(len(clipped_axes), 2 * metric_count)
        self.assertEqual(
            clipped_axes[0].get_ylim(),
            clipped_axes[metric_count].get_ylim(),
        )
        self.assertEqual(
            [tick.get_text() for tick in clipped_axes[0].get_xticklabels()],
            ["Saved\nn=10", "Stopped\nn=10"],
        )

if __name__ == "__main__":
    unittest.main()
