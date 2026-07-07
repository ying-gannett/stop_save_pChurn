from IPython.display import display
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


DEFAULT_PERCENTILES = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
SLICE_FIELDS = ["contact_channels", "cohort", "src_risk_tier", "contact_timing"]
ORDERS = {
    "src_risk_tier": ['1. Low', '2. Med-Low', '3. Medium', '4. Med-High', '5. High'],
    "cohort": ['Two-Offer Cohort', 'Three-Offer Cohort'],
    "Treatment": ["Control", "Midpoint", "Tiered"],
    "contact_channel": ['No Action yet', 'Called-In Cancel Flow', 'Online Cancel Flow', 'Called-In first', 'Online first'],
    "status": ['No Action yet', 'saved', 'stoped'],
    "contact_timing": ['Contact Before Pricing', 'Contact On/After Pricing'],
    "repeatedly_called": [0, 1]
}

def cast_numeric_fields(data, numeric_fields):
    for field in numeric_fields:
        data[field] = data[field].astype("float64")
    return data


def build_distribution_summary(data, numeric_fields, percentiles=DEFAULT_PERCENTILES):
    quality_checks = []
    for field in numeric_fields:
        quality_checks.append({
            "field": field,
            "row_count": len(data),
            "null_count": data[field].isna().sum(),
            "null_pct": data[field].isna().mean() * 100,
            "zero_count": (data[field] == 0).sum(),
            "negative_count": (data[field] < 0).sum(),
            "min": data[field].min(),
            "max": data[field].max(),
            "mean": data[field].mean(),
            "median": data[field].median(),
        })

    quality_summary = pd.DataFrame(quality_checks).set_index("field")
    summary_stats = data[numeric_fields].agg(["std"]).T
    percentile_summary = data[numeric_fields].quantile(percentiles).T
    percentile_summary.columns = [f"p{int(p * 100):02d}" for p in percentiles]

    return quality_summary.join(summary_stats).join(percentile_summary).reset_index()


def build_outlier_summary(data, numeric_fields):
    """Vis - Boxplot
    Build a summary of outliers for numeric fields based on the IQR method."""
    outlier_summary = []

    for field in numeric_fields:
        q1 = data[field].quantile(0.25)
        q3 = data[field].quantile(0.75)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outlier_count = ((data[field] < lower_bound) | (data[field] > upper_bound)).sum()

        outlier_summary.append({
            "field": field,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "outlier_count": outlier_count,
            "outlier_pct": outlier_count / len(data) * 100,
        })

    return pd.DataFrame(outlier_summary)


def build_segment_summary(data, segment, id_col="billing_account"):
    return (
        data.groupby(segment)
        .agg(
            users=(id_col, "count"),
            avg_frequency=("frequency", "mean"),
            median_frequency=("frequency", "median"),
            p90_frequency=("frequency", lambda x: x.quantile(0.90)),
            avg_breadth=("breadth", "mean"),
            median_breadth=("breadth", "median"),
            p90_breadth=("breadth", lambda x: x.quantile(0.90)),
            avg_tenure=("tenure", "mean"),
            median_tenure=("tenure", "median"),
            avg_total_cost=("tt_cost", "mean"),
            median_total_cost=("tt_cost", "median"),
            p90_total_cost=("tt_cost", lambda x: x.quantile(0.90)),
        )
        .reset_index()
        .sort_values("users", ascending=False)
    )


def __get_group_counts(data, group_col, id_col="billing_account", dropna=True):
    return data.groupby(group_col, dropna=dropna)[id_col].count()


def __resolve_group_order(counts, order=None, min_n=1):
    valid_groups = counts[counts >= min_n].index.tolist()

    if order is None:
        return counts[counts >= min_n].sort_values(ascending=False).index.tolist()

    resolved_order = [group for group in order if group in valid_groups]
    resolved_order += [group for group in valid_groups if group not in resolved_order]
    return resolved_order


def plot_one_metric_by_group(
    data,
    metric,
    group_col,
    order=None,
    title=None,
    xlabel=None,
    ylabel=None,
    id_col="billing_account",
    min_n=1,
    show_counts=True,
    show_points=True,
    showfliers=True,
    rotate_xticks=False,
    figsize=(8, 5),
    point_kwargs=None,
    display_counts_on_empty=False,
    ax=None,
    show=True,
):
    required_cols = [metric, group_col, id_col]
    missing_cols = [col for col in required_cols if col not in data.columns]
    if missing_cols:
        print(f"Missing required columns: {missing_cols}")
        return None

    plot_df = data.dropna(subset=[metric, group_col]).copy()
    counts = __get_group_counts(plot_df, group_col, id_col=id_col)
    resolved_order = __resolve_group_order(counts, order=order, min_n=min_n)
    plot_df = plot_df[plot_df[group_col].isin(resolved_order)]

    if plot_df.empty or not resolved_order:
        if display_counts_on_empty:
            print("No groups meet the minimum sample size.")
            display(counts.rename("users").reset_index().sort_values("users", ascending=False))
        return None

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.figure

    sns.boxplot(
        data=plot_df,
        x=group_col,
        y=metric,
        order=resolved_order,
        showfliers=showfliers,
        ax=ax,
    )

    if show_points:
        stripplot_kwargs = {
            "color": "black",
            "alpha": 0.25,
            "size": 3,
            "jitter": 0.2,
        }
        if point_kwargs is not None:
            stripplot_kwargs.update(point_kwargs)

        sns.stripplot(
            data=plot_df,
            x=group_col,
            y=metric,
            order=resolved_order,
            ax=ax,
            **stripplot_kwargs,
        )

    if show_counts:
        labels = [f"{group}\n(n={counts.loc[group]:,})" for group in resolved_order]
        ax.set_xticks(range(len(resolved_order)))
        ax.set_xticklabels(
            labels,
            rotation=45 if rotate_xticks else 0,
            ha="right" if rotate_xticks else "center",
        )
    elif rotate_xticks:
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.set_title(title or f"{metric} by {group_col}")
    ax.set_xlabel(xlabel or group_col)
    ax.set_ylabel(ylabel or metric)

    if show:
        fig.tight_layout()
        plt.show()

    return ax


def plot_metrics_by_group(
    data,
    metrics,
    group_col,
    title_template=None,
    n_cols=2,
    figsize=(8, 5),
    **kwargs,
):
    metrics = list(metrics)
    if not metrics:
        print("No metrics provided.")
        return None

    if n_cols < 1:
        raise ValueError("n_cols must be at least 1.")

    n_cols = min(n_cols, len(metrics))
    n_rows = int(np.ceil(len(metrics) / n_cols))
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(figsize[0] * n_cols, figsize[1] * n_rows),
        squeeze=False,
    )

    plotted_axes = []
    for metric, ax in zip(metrics, axes.flat):
        title = None
        if title_template is not None:
            title = title_template.format(metric=metric, group_col=group_col)

        plotted_ax = plot_one_metric_by_group(
            data=data,
            metric=metric,
            group_col=group_col,
            title=title,
            ax=ax,
            show=False,
            **kwargs,
        )

        if plotted_ax is None:
            ax.set_visible(False)
        else:
            plotted_axes.append(plotted_ax)

    for ax in axes.flat[len(metrics):]:
        ax.set_visible(False)

    if not plotted_axes:
        plt.close(fig)
        print("No groups meet the minimum sample size.")
        return None

    fig.tight_layout()
    plt.show()
    return axes


def plot_histogram_with_log(data, metric, group, bins=50, figsize=(10, 5)):
    """Vis - Histogram
    Plot histogram of a metric and its log-transformed version side by side."""
    values = data[metric].dropna()
    non_negative_values = values[values >= 0]

    fig, axes = plt.subplots(1, 2, figsize=figsize)

    sns.histplot(values, bins=bins, kde=True, ax=axes[0])
    axes[0].set_title(f"Distribution of {metric} | {group} users")
    axes[0].set_xlabel(metric)
    axes[0].set_ylabel("Count")

    sns.histplot(np.log1p(non_negative_values), bins=bins, kde=True, ax=axes[1])
    axes[1].set_title(f"Log-Scale Distribution of {metric} | {group} users")
    axes[1].set_xlabel(f"log1p({metric})")
    axes[1].set_ylabel("Count")

    plt.tight_layout()
    plt.show()
    return axes


def plot_full_and_clipped_boxplot(data, metric, group, lower_q=0.01, upper_q=0.99, figsize=(10, 3)):
    """Vis - Boxplot
    Plot boxplot of a metric and its clipped version side by side."""
    values = data[metric].dropna()
    lower = values.quantile(lower_q)
    upper = values.quantile(upper_q)
    clipped_values = values.clip(lower, upper)

    fig, axes = plt.subplots(1, 2, figsize=figsize)

    sns.boxplot(x=values, ax=axes[0])
    axes[0].set_title(f"Box Plot of {metric} | {group} users")
    axes[0].set_xlabel(metric)

    sns.boxplot(x=clipped_values, ax=axes[1])
    axes[1].set_title(f"Box Plot of {metric}, Clipped to p01-p99 | {group} users")
    axes[1].set_xlabel(f"{metric} clipped to p01-p99")

    plt.tight_layout()
    plt.show()
    return axes


def plot_scatter_pairs(data, pairs, sample_size=10000, random_state=42):
    if data.empty:
        print("No rows available for scatter plots.")
        return

    sample_df = data.sample(min(sample_size, len(data)), random_state=random_state)

    for x_col, y_col in pairs:
        if x_col in sample_df.columns and y_col in sample_df.columns:
            plt.figure(figsize=(8, 5))
            sns.scatterplot(data=sample_df, x=x_col, y=y_col, alpha=0.3)
            plt.title(f"{y_col} vs {x_col}")
            plt.xlabel(x_col)
            plt.ylabel(y_col)
            plt.tight_layout()
            plt.show()


def plot_bucket_counts(data, bucket_col, dropna=False, figsize=(9, 5)):
    bucket_counts = data[bucket_col].value_counts(dropna=dropna).reset_index()
    bucket_counts.columns = [bucket_col, "count"]

    plt.figure(figsize=figsize)
    ax = sns.barplot(data=bucket_counts, x=bucket_col, y="count")
    ax.set_title(f"Distribution by {bucket_col}")
    ax.set_xlabel(bucket_col)
    ax.set_ylabel("Count")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

    return bucket_counts


def __apply_segment_filters(data, filters):
    filtered = data.copy()

    for col, value in filters.items():
        if not pd.isna(value):
            filtered = filtered[filtered[col].eq(value)]

    return filtered


def build_segment_combo_counts(
    data,
    metrics,
    slice_fields,
    group_col="Treatment",
    action_status="No Action yet",
    id_col="billing_account",
    min_n=5,
    treatment_order=ORDERS["Treatment"],
):
    action_data = data[data["status"].ne(action_status)].copy()
    segment_combo_counts = (action_data
        .groupby(slice_fields + [group_col], dropna=False)
        .agg(users=(id_col, "count"))
        .reset_index()
    )
    display(segment_combo_counts.head(5))

    plot_combo = segment_combo_counts.iloc[:, :4].drop_duplicates()
    print(f"Plotting {len(plot_combo)} unique segment combinations...")
    for row in plot_combo.itertuples(index=False):
        filters = row._asdict()

        plot_df = __apply_segment_filters(action_data, filters)
        title_filters = ", ".join(
            f"{value}" for _, value in filters.items() if value is not None
        ) or "All action users"
        plot_metrics_by_group(
            data=plot_df,
            metrics=metrics,
            group_col=group_col,
            order=treatment_order,
            title_template=f"{title_filters}\n\n{{metric}} by {group_col}",
            xlabel=group_col,
            min_n=min_n,
            figsize=(9, 5),
            display_counts_on_empty=False,
        )
    return segment_combo_counts