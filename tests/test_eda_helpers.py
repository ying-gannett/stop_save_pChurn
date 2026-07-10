import unittest
import warnings

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
import numpy as np
import pandas as pd

from src import eda_helpers


class MetricBoxplotViewsTests(unittest.TestCase):
    fit_reference = staticmethod(
        getattr(eda_helpers, "_fit_global_metric_reference")
    )
    prepare_boxplot_data = staticmethod(
        getattr(eda_helpers, "_prepare_metric_boxplot_data")
    )
    resolve_palette = staticmethod(
        getattr(eda_helpers, "_resolve_metric_group_palette")
    )

    def setUp(self):
        warnings.filterwarnings(
            "ignore",
            message="vert: bool was deprecated.*",
            category=matplotlib.MatplotlibDeprecationWarning,
        )
        self.data = pd.DataFrame(
            {
                "billing_account": [f"acct_{index:02d}" for index in range(12)],
                "Treatment": ["Control", "Midpoint", "Tiered"] * 4,
                "segment": ["A"] * 6 + ["B"] * 6,
                "status": ["saved"] * 12,
                "frequency": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 100],
                "tenure": [
                    100,
                    200,
                    300,
                    400,
                    500,
                    600,
                    700,
                    800,
                    900,
                    1000,
                    1100,
                    10000,
                ],
                "constant_metric": [5.0] * 12,
            }
        )
        self.metrics = ["frequency", "tenure", "constant_metric"]

    def tearDown(self):
        plt.close("all")

    def test_preparation_returns_raw_clipped_and_standardized_clipped_values(self):
        reference = self.fit_reference(
            self.data,
            self.metrics,
            lower_q=0.1,
            upper_q=0.9,
        )
        segment_data = self.data[self.data["segment"].eq("A")]
        prepared = self.prepare_boxplot_data(
            segment_data,
            self.metrics,
            lower_q=0.1,
            upper_q=0.9,
            metric_reference=reference,
        )
        full_plot, clipped_plot, standardized_plot, _, _ = prepared

        metric = "frequency"
        raw_values = segment_data[metric]
        expected_clipped = raw_values.clip(
            reference["lower_bounds"][metric],
            reference["upper_bounds"][metric],
        )
        expected_standardized = (
            expected_clipped
            .sub(reference["centers"][metric])
            .div(reference["spreads"][metric])
        )

        actual_full = full_plot.loc[full_plot["metric"].eq(metric), "value"]
        actual_clipped = clipped_plot.loc[
            clipped_plot["metric"].eq(metric),
            "value",
        ]
        actual_standardized = standardized_plot.loc[
            standardized_plot["metric"].eq(metric),
            "value",
        ]
        np.testing.assert_array_equal(actual_full, raw_values)
        np.testing.assert_allclose(actual_clipped, expected_clipped)
        np.testing.assert_allclose(actual_standardized, expected_standardized)

    def test_zero_iqr_metric_preserves_raw_values_and_standardizes_to_zero(self):
        reference = self.fit_reference(self.data, self.metrics)
        self.assertEqual(reference["spreads"]["constant_metric"], 1.0)

        prepared = self.prepare_boxplot_data(
            self.data,
            self.metrics,
            metric_reference=reference,
        )
        full_plot, clipped_plot, standardized_plot, _, _ = prepared

        def metric_values(plot_data):
            return plot_data.loc[
                plot_data["metric"].eq("constant_metric"),
                "value",
            ]

        np.testing.assert_array_equal(metric_values(full_plot), 5.0)
        np.testing.assert_array_equal(metric_values(clipped_plot), 5.0)
        np.testing.assert_array_equal(metric_values(standardized_plot), 0.0)

    def test_standalone_boxplot_function_renders_all_three_views(self):
        axes = eda_helpers.plot_metric_boxplot_views(
            self.data,
            metrics=self.metrics,
            group_col="Treatment",
            group_order=["Control", "Midpoint", "Tiered"],
            min_n=1,
            show_points=False,
            show=False,
            save=False,
            close=False,
            boxplot_kwargs={"saturation": 1},
        )

        self.assertEqual(len(axes), 3)
        self.assertIn("Original Values", axes[0].get_title())
        self.assertIn("Clipped Values", axes[1].get_title())
        self.assertIn("Standardized Clipped Values", axes[2].get_title())
        self.assertEqual(axes[0].get_ylabel(), "Value")
        self.assertEqual(axes[1].get_ylabel(), "Value")
        self.assertEqual(
            axes[2].get_ylabel(),
            "Value relative to global median (IQR units)",
        )
        self.assertIsNone(axes[0].get_legend())
        self.assertIsNone(axes[1].get_legend())
        self.assertIsNotNone(axes[2].get_legend())
        treatment_colors = ["#2E8B57", "#4C78A8", "#8E5EA2"]
        legend_colors = [
            handle.get_facecolor()
            for handle in axes[2].get_legend().legend_handles
        ]
        for actual, expected in zip(legend_colors, treatment_colors):
            np.testing.assert_allclose(actual, to_rgba(expected))

    def test_semantic_status_colors_and_explicit_palette_override(self):
        status_palette = self.resolve_palette(
            "status",
            ["Saved", "Stoped", "No Action yet"],
        )
        self.assertEqual(status_palette["Saved"], "#2E8B57")
        self.assertEqual(status_palette["Stoped"], "#C44E52")
        self.assertEqual(status_palette["No Action yet"], "#8C8C8C")

        custom_palette = {
            "Control": "black",
            "Midpoint": "gray",
            "Tiered": "white",
        }
        self.assertIs(
            self.resolve_palette(
                "Treatment",
                ["Control", "Midpoint", "Tiered"],
                palette=custom_palette,
            ),
            custom_palette,
        )

    def test_user_counts_are_unique_and_missing_segments_are_retained(self):
        duplicated = pd.concat([self.data, self.data.iloc[[0]]], ignore_index=True)
        duplicated.loc[len(duplicated)] = duplicated.iloc[1]
        duplicated.loc[len(duplicated) - 1, "segment"] = np.nan

        summary = eda_helpers.build_segment_summary(
            duplicated,
            "segment",
            self.metrics,
        )
        segment_a_users = summary.loc[summary["segment"].eq("A"), "users"].item()
        missing_segment_users = summary.loc[summary["segment"].isna(), "users"].item()
        self.assertEqual(segment_a_users, 6)
        self.assertEqual(missing_segment_users, 1)

        prepared = self.prepare_boxplot_data(
            duplicated,
            self.metrics,
            group_col="Treatment",
            group_order=["Control", "Midpoint", "Tiered"],
        )
        group_counts = prepared[3]
        self.assertEqual(group_counts["Control"], 4)

    def test_outlier_percentage_uses_non_null_denominator(self):
        summary = eda_helpers.build_outlier_summary(
            pd.DataFrame({"metric": [1.0, 1.0, 1.0, 100.0, np.nan]}),
            ["metric"],
        ).iloc[0]

        self.assertEqual(summary["row_count"], 5)
        self.assertEqual(summary["non_null_count"], 4)
        self.assertEqual(summary["outlier_count"], 1)
        self.assertEqual(summary["outlier_pct"], 25.0)

    def test_segment_plotter_does_not_filter_status_values(self):
        data = self.data.copy()
        data.loc[0, "status"] = "No Action yet"
        counts = eda_helpers.plot_slices_of_segments_boxplot(
            data,
            metrics=self.metrics,
            slice_fields=["status"],
            group_col=None,
            show_points=False,
            show=False,
            save=False,
            close=True,
        )

        self.assertIn("No Action yet", counts["status"].tolist())

    def test_segment_outputs_share_separate_limits_across_pages(self):
        counts = eda_helpers.plot_slices_of_segments_boxplot(
            self.data,
            metrics=self.metrics,
            slice_fields=["segment"],
            min_n=1,
            slices_per_file=1,
            n_cols=2,
            show_points=False,
            show=False,
            save=False,
            close=False,
        )

        self.assertEqual(
            set(counts.attrs["saved_paths"]),
            {"full", "clipped", "standardized_clipped"},
        )
        figures = [plt.figure(number) for number in plt.get_fignums()]
        self.assertEqual(len(figures), 6)
        limits_by_plot_type = {
            "full": [],
            "clipped": [],
            "standardized_clipped": [],
        }

        for figure in figures:
            figure_title = figure._suptitle.get_text()
            visible_axes = [axis for axis in figure.axes if axis.get_visible()]
            self.assertEqual(len(visible_axes), 1)
            axis = visible_axes[0]

            if "Standardized clipped" in figure_title:
                plot_type = "standardized_clipped"
                expected_label = "Value relative to global median (IQR units)"
            elif "Clipped values" in figure_title:
                plot_type = "clipped"
                expected_label = "Value"
            else:
                plot_type = "full"
                expected_label = "Value"

            limits_by_plot_type[plot_type].append(axis.get_ylim())
            self.assertEqual(axis.get_ylabel(), expected_label)

        for plot_limits in limits_by_plot_type.values():
            self.assertEqual(len(plot_limits), 2)
            self.assertEqual(plot_limits[0], plot_limits[1])

        self.assertNotEqual(
            limits_by_plot_type["clipped"][0],
            limits_by_plot_type["standardized_clipped"][0],
        )

    def test_invalid_quantile_order_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "lower_q and upper_q"):
            eda_helpers.plot_slices_of_segments_boxplot(
                self.data,
                metrics=self.metrics,
                slice_fields=["segment"],
                lower_q=0.9,
                upper_q=0.1,
                save=False,
            )

    def test_legacy_boxplot_name_remains_an_alias(self):
        self.assertIs(
            eda_helpers.plot_full_and_clipped_boxplot,
            eda_helpers.plot_metric_boxplot_views,
        )


if __name__ == "__main__":
    unittest.main()
