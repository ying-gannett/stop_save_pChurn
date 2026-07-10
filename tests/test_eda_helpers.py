import unittest

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src import eda_helpers


class RobustMetricScaleTests(unittest.TestCase):
    fit_scale = staticmethod(
        getattr(eda_helpers, "__fit_global_robust_metric_scale")
    )
    prepare_boxplot_data = staticmethod(
        getattr(eda_helpers, "__prepare_metric_boxplot_data")
    )

    def setUp(self):
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

    def test_full_and_clipped_values_use_the_same_global_robust_scale(self):
        scale_params = self.fit_scale(
            self.data,
            self.metrics,
            lower_q=0.1,
            upper_q=0.9,
        )

        prepared = self.prepare_boxplot_data(
            self.data[self.data["segment"].eq("A")],
            self.metrics,
            lower_q=0.1,
            upper_q=0.9,
            metric_scale_params=scale_params,
        )
        full_plot, clipped_plot, _, _ = prepared

        metric = "frequency"
        center = scale_params["centers"][metric]
        spread = scale_params["spreads"][metric]
        expected_full = (
            self.data.loc[self.data["segment"].eq("A"), metric] - center
        ) / spread
        expected_clipped = (
            self.data.loc[self.data["segment"].eq("A"), metric]
            .clip(
                scale_params["lower_bounds"][metric],
                scale_params["upper_bounds"][metric],
            )
            .sub(center)
            .div(spread)
        )

        actual_full = full_plot.loc[full_plot["metric"].eq(metric), "value"]
        actual_clipped = clipped_plot.loc[
            clipped_plot["metric"].eq(metric),
            "value",
        ]
        np.testing.assert_allclose(actual_full, expected_full)
        np.testing.assert_allclose(actual_clipped, expected_clipped)

    def test_zero_iqr_metric_has_a_safe_fallback_scale(self):
        scale_params = self.fit_scale(
            self.data,
            self.metrics,
        )

        self.assertEqual(scale_params["spreads"]["constant_metric"], 1.0)
        prepared = self.prepare_boxplot_data(
            self.data,
            self.metrics,
            metric_scale_params=scale_params,
        )
        full_plot, _, _, _ = prepared
        constant_values = full_plot.loc[
            full_plot["metric"].eq("constant_metric"),
            "value",
        ]
        np.testing.assert_array_equal(constant_values, np.zeros(len(constant_values)))

    def test_raw_mode_keeps_original_values_and_segment_level_clipping(self):
        segment_data = self.data[self.data["segment"].eq("A")]
        prepared = self.prepare_boxplot_data(
            segment_data,
            self.metrics,
            lower_q=0.1,
            upper_q=0.9,
        )
        full_plot, clipped_plot, _, _ = prepared

        metric = "frequency"
        raw_values = segment_data[metric]
        expected_clipped = raw_values.clip(
            raw_values.quantile(0.1),
            raw_values.quantile(0.9),
        )
        actual_full = full_plot.loc[full_plot["metric"].eq(metric), "value"]
        actual_clipped = clipped_plot.loc[
            clipped_plot["metric"].eq(metric),
            "value",
        ]
        np.testing.assert_array_equal(actual_full, raw_values)
        np.testing.assert_allclose(actual_clipped, expected_clipped)

    def test_standardized_segment_plots_share_limits_and_describe_scale(self):
        counts = eda_helpers.plot_slices_of_segments_boxplot(
            self.data,
            metrics=self.metrics,
            slice_fields=["segment"],
            min_n=1,
            slices_per_file=1,
            n_cols=2,
            metric_scale="robust_global",
            show_points=False,
            show=False,
            save=False,
            close=False,
        )

        self.assertEqual(counts.attrs["metric_scale"], "robust_global")
        figures = [plt.figure(number) for number in plt.get_fignums()]
        self.assertEqual(len(figures), 4)
        limits_by_plot_type = {"full": [], "clipped": []}
        for figure in figures:
            figure_title = figure._suptitle.get_text()
            self.assertIn("Standardized", figure_title)
            visible_axes = [axis for axis in figure.axes if axis.get_visible()]
            self.assertEqual(len(visible_axes), 1)
            plot_type = "clipped" if "clipped" in figure_title else "full"
            limits_by_plot_type[plot_type].append(visible_axes[0].get_ylim())
            self.assertTrue(
                all(
                    axis.get_ylabel()
                    == "Value relative to global median (IQR units)"
                    for axis in visible_axes
                )
            )
        for plot_limits in limits_by_plot_type.values():
            self.assertEqual(plot_limits[0], plot_limits[1])

    def test_invalid_metric_scale_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "metric_scale"):
            eda_helpers.plot_slices_of_segments_boxplot(
                self.data,
                metrics=self.metrics,
                slice_fields=["segment"],
                metric_scale="per_segment",
                save=False,
            )


if __name__ == "__main__":
    unittest.main()
