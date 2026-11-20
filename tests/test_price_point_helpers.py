import unittest

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

from src.price_point_helpers import plot_metric, summarize_price_points


class PricePointHelpersTests(unittest.TestCase):
    def setUp(self):
        self.data = pd.DataFrame(
            {
                "id_subscrip": [1, 2, 3, 4, 5, 6],
                "cohort": ["Three"] * 4 + ["Two"] * 2,
                "treatment": ["Control"] * 3 + ["Midpoint"] * 3,
                "market": ["A", "A", "B", "B", "B", "C"],
                "contacted": [0, 1, 1, 1, 1, 1],
                "churn_code": [None, 0, 1, 0, 1, 0],
                "gamer_type": [
                    "Not Gamer",
                    "Not Gamer",
                    "Not Gamer",
                    "Repeat Restart",
                    "Seek lower rate, still paid offered rate",
                    "Seek and paid lower rate",
                ],
                "revenue_single_saved": [0, 20, 0, 10, 5, 15],
            }
        )

    def tearDown(self):
        plt.close("all")

    def test_summary_calculates_metrics(self):
        overall = summarize_price_points(self.data).iloc[0]
        self.assertEqual(overall["users"], 6)
        self.assertAlmostEqual(overall["contact_rate"], 5 / 6)
        self.assertAlmostEqual(overall["churn_rate"], 2 / 5)
        self.assertAlmostEqual(overall["gamer_rate"], 3 / 5)
        self.assertAlmostEqual(overall["cnrc"], 10)

    def test_summary_accepts_multiple_dimensions(self):
        summary = summarize_price_points(
            self.data, ["cohort", "treatment", "market"]
        )
        self.assertEqual(len(summary), 5)

    def test_plot_uses_summary(self):
        summary = summarize_price_points(self.data, ["cohort", "treatment"])
        ax = plot_metric(summary, "contact_rate", "cohort", "treatment")
        self.assertEqual(ax.get_title(), "contact_rate")


if __name__ == "__main__":
    unittest.main()
