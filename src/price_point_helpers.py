"""Reusable summaries and charts for the price-point notebook."""

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, PercentFormatter
import seaborn as sns


def summarize_price_points(data, dimensions=()):
    """Calculate price-point metrics for the requested grouping dimensions."""
    dimensions = list(dimensions)
    if not dimensions:
        data = data.assign(overall="Overall")
        dimensions = ["overall"]

    summary = (
        data.groupby(dimensions, observed=True, dropna=False)
        .agg(
            users=("id_subscrip", "count"),
            contacted_users=("contacted", "sum"),
            saved_users=("churn_code", lambda values: values.eq(0).sum()),
            stopped_users=("churn_code", lambda values: values.eq(1).sum()),
            gamer_users=("gamer_type", lambda values: values.ne("Not Gamer").sum()),
            total_revenue=("revenue_single_saved", "sum"),
        )
        .reset_index()
    )
    summary["contact_rate"] = summary["contacted_users"] / summary["users"]
    summary["churn_rate"] = summary["stopped_users"] / summary["contacted_users"]
    summary["save_rate"] = summary["saved_users"] / summary["contacted_users"]
    summary["gamer_rate"] = summary["gamer_users"] / summary["contacted_users"]
    summary["cnrc"] = summary["total_revenue"] / summary["contacted_users"]
    return summary


def plot_metric(summary, metric, x, hue=None, title=None):
    """Plot one summary metric as a grouped bar chart."""
    fig, ax = plt.subplots(figsize=(11, 5.5))
    sns.barplot(data=summary, x=x, y=metric, hue=hue, ax=ax)
    if metric.endswith("_rate"):
        ax.yaxis.set_major_formatter(PercentFormatter(1))
    elif metric == "cnrc":
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"${value:,.2f}"))
    ax.set_title(title or metric)
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    return ax
