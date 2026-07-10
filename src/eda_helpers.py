import re
from pathlib import Path

from IPython.display import display
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from typing import Any, Callable


DEFAULT_PERCENTILES = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
SLICE_FIELDS = ["contact_channels", "cohort", "src_risk_tier", "contact_timing"]
ORDERS = {
    "src_risk_tier": ['1. Low risk', '2. Med-Low risk', '3. Medium risk', '4. Med-High risk', '5. High risk'],
    "cohort": ['Two-Offer Cohort', 'Three-Offer Cohort'],
    "Treatment": ["Control", "Midpoint", "Tiered"],
    "contact_channel": ['No Action yet', 'Called-In Cancel Flow', 'Online Cancel Flow', 'Called-In first', 'Online first'],
    "status": ['No Action yet', 'saved', 'stoped'],
    "contact_timing": ['Contact Before Pricing', 'Contact On/After Pricing'],
    "repeatedly_called": [0, 1]
}

# Chart output helpers


def __slugify_file_name(value, max_length=120):
    """Convert a title or file name to a filesystem-friendly stem."""
    value = str(value).strip()
    value = re.sub(r"[^\w\s.-]", "", value)
    value = re.sub(r"[\s.-]+", "_", value)
    value = value.strip("_")
    return (value or "chart")[:max_length].rstrip("_")


def __resolve_chart_path(folder, file_name, extension):
    """Build the output path for a chart file."""
    extension = extension.lstrip(".")
    file_path = Path(file_name)
    suffix = file_path.suffix or f".{extension}"
    stem = __slugify_file_name(file_path.stem if file_path.suffix else file_path.name)
    return Path(folder) / f"{stem}{suffix}"


def __get_unique_path(path):
    """Return a non-existing path by appending a numeric suffix when needed."""
    if not path.exists():
        return path

    counter = 1
    while True:
        candidate = path.with_name(f"{path.stem}_{counter}{path.suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def save_chart(
    chart=None,
    folder="charts",
    file_name=None,
    chart_title=None,
    extension="png",
    dpi=300,
    bbox_inches="tight",
    overwrite=True,
    **savefig_kwargs,
):
    """Save a matplotlib figure using an explicit file name or chart title."""
    if chart is None:
        fig = plt.gcf()
    elif hasattr(chart, "savefig"):
        fig = chart
    elif hasattr(chart, "figure"):
        fig = chart.figure
    else:
        raise ValueError("chart must be a matplotlib Figure, Axes, or None.")

    output_folder = Path(folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    resolved_title = file_name or chart_title or "chart"
    output_path = __resolve_chart_path(output_folder, resolved_title, extension)
    if not overwrite:
        output_path = __get_unique_path(output_path)

    fig.savefig(output_path, dpi=dpi, bbox_inches=bbox_inches, **savefig_kwargs)
    return output_path


def __finalize_chart(
    fig,
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
):
    """Apply layout, optionally save/show a chart, and close it when appropriate."""
    fig.tight_layout()

    saved_path = None
    if save:
        saved_path = save_chart(
            fig,
            folder=chart_folder,
            file_name=file_name,
            chart_title=chart_title,
            **(save_kwargs or {}),
        )

    if show:
        plt.show()

    should_close = not show if close is None else close
    if should_close:
        plt.close(fig)

    return saved_path


def __format_file_name_template(file_name, **values):
    """Render a file name template with known values when placeholders are present."""
    if file_name is None:
        return None

    try:
        return str(file_name).format(**values)
    except (KeyError, IndexError):
        return file_name


def __append_file_name_suffix(file_name, suffix):
    """Append a slugified suffix to a file name before its extension."""
    path = Path(file_name)
    suffix = __slugify_file_name(suffix)
    if path.suffix:
        return f"{path.stem}_{suffix}{path.suffix}"
    return f"{path.name}_{suffix}"


def cast_numeric_fields(data, numeric_fields):
    """Cast selected columns to float64 in place and return the dataframe."""
    for field in numeric_fields:
        data[field] = data[field].astype("float64")
    return data


def build_distribution_summary(data, numeric_fields, percentiles=DEFAULT_PERCENTILES):
    """Summarize quality checks, standard deviation, and percentiles for numeric fields."""
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
    """Summarize IQR-based outlier counts and bounds for numeric fields."""
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


def build_segment_summary(data, segment, metrics, id_col="billing_account"):
    """Aggregate user counts and numeric metric summaries by one segment."""
    # Allow the aggregation function to be a string or a lambda
    agg_spec: dict[str, tuple[str, str | Callable[[Any], Any]]] = {"users": (id_col, "count")}
    for metric in metrics:
        agg_spec[f"avg_{metric}"] = (metric, "mean")
        agg_spec[f"median_{metric}"] = (metric, "median")
        agg_spec[f"p90_{metric}"] = (metric, lambda x: x.quantile(0.90))

    return (
        data.groupby(segment)
        .agg(**agg_spec)
        .reset_index()
        .sort_values("users", ascending=False)
    )


def __get_group_counts(data, group_col, id_col="billing_account", dropna=True):
    """Count rows or accounts per group for plotting filters and labels."""
    return data.groupby(group_col, dropna=dropna)[id_col].count()


def __resolve_group_order(counts, order=None, min_n=1):
    """Resolve group display order after applying a minimum sample-size threshold."""
    valid_groups = counts[counts >= min_n].index.tolist()

    if order is None:
        return counts[counts >= min_n].sort_values(ascending=False).index.tolist()

    resolved_order = [group for group in order if group in valid_groups]
    resolved_order += [group for group in valid_groups if group not in resolved_order]
    return resolved_order


def plot_histogram_with_log(
    data,
    metric,
    group,
    bins=50,
    figsize=(10, 5),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
):
    """Plot raw and log1p histograms for one numeric metric."""
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

    __finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        chart_title=chart_title or f"{metric} distribution | {group} users",
        close=close,
        save_kwargs=save_kwargs,
    )
    return axes


def plot_correlation_heatmap(
    data,
    numeric_fields,
    group=None,
    figsize=(7, 5),
    annot=True,
    cmap="coolwarm",
    center=0,
    fmt=".2f",
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
    **heatmap_kwargs,
):
    """Plot a correlation heatmap for available numeric fields."""
    fields = [field for field in numeric_fields if field in data.columns]
    if not fields:
        print("No numeric fields available for correlation heatmap.")
        return None

    missing_fields = [field for field in numeric_fields if field not in data.columns]
    if missing_fields:
        print(f"Missing numeric fields skipped: {missing_fields}")

    title = chart_title or (
        f"Correlation Matrix | {group} users" if group is not None else "Correlation Matrix"
    )

    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(
        data[fields].corr(),
        annot=annot,
        cmap=cmap,
        center=center,
        fmt=fmt,
        ax=ax,
        **heatmap_kwargs,
    )
    ax.set_title(title)

    __finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        chart_title=title,
        close=close,
        save_kwargs=save_kwargs,
    )
    return ax


def plot_scatter_pairs(
    data,
    pairs,
    sample_size=10000,
    random_state=42,
    figsize=(8, 5),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
):
    """Plot and save scatter charts for selected numeric column pairs."""
    if data.empty:
        print("No rows available for scatter plots.")
        return []

    sample_df = data.sample(min(sample_size, len(data)), random_state=random_state)
    pairs = list(pairs)
    saved_paths = []
    plotted_count = 0

    for x_col, y_col in pairs:
        if x_col in sample_df.columns and y_col in sample_df.columns:
            plotted_count += 1
            title = __format_file_name_template(
                chart_title,
                x_col=x_col,
                y_col=y_col,
                pair=f"{y_col}_vs_{x_col}",
                index=plotted_count,
            ) or f"{y_col} vs {x_col}"
            fig, ax = plt.subplots(figsize=figsize)
            sns.scatterplot(data=sample_df, x=x_col, y=y_col, alpha=0.3, ax=ax)
            ax.set_title(title)
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)

            resolved_file_name = __format_file_name_template(
                file_name,
                x_col=x_col,
                y_col=y_col,
                pair=f"{y_col}_vs_{x_col}",
                index=plotted_count,
            )
            if file_name is not None and resolved_file_name == file_name and len(pairs) > 1:
                resolved_file_name = __append_file_name_suffix(
                    file_name,
                    f"{y_col}_vs_{x_col}",
                )
            elif file_name is None and chart_title is not None and len(pairs) > 1:
                resolved_file_name = __append_file_name_suffix(
                    title,
                    f"{y_col}_vs_{x_col}",
                )

            saved_path = __finalize_chart(
                fig,
                show=show,
                save=save,
                chart_folder=chart_folder,
                file_name=resolved_file_name,
                chart_title=title,
                close=close,
                save_kwargs=save_kwargs,
            )
            if saved_path is not None:
                saved_paths.append(saved_path)

    if plotted_count == 0:
        print("No valid scatter plot column pairs found.")

    return saved_paths


def plot_bucket_counts(
    data,
    bucket_col,
    dropna=False,
    figsize=(9, 5),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
):
    """Plot frequency counts for a categorical or bucketed column."""
    bucket_counts = data[bucket_col].value_counts(dropna=dropna).reset_index()
    bucket_counts.columns = [bucket_col, "count"]

    fig, ax = plt.subplots(figsize=figsize)
    ax = sns.barplot(data=bucket_counts, x=bucket_col, y="count", ax=ax)
    title = chart_title or f"Distribution by {bucket_col}"
    ax.set_title(title)
    ax.set_xlabel(bucket_col)
    ax.set_ylabel("Count")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    saved_path = __finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        chart_title=title,
        close=close,
        save_kwargs=save_kwargs,
    )
    if saved_path is not None:
        bucket_counts.attrs["saved_path"] = str(saved_path)

    return bucket_counts


def __apply_segment_filters(data, filters):
    """Filter a dataframe to rows matching a segment-combination dictionary."""
    filtered = data.copy()

    for col, value in filters.items():
        if not pd.isna(value):
            filtered = filtered[filtered[col].eq(value)]

    return filtered


def plot_full_and_clipped_boxplot(
    data,
    metrics,
    dataset_name=None,
    group_col=None,
    group_order=None,
    lower_q=0.01,
    upper_q=0.99,
    figsize=(10, 4),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
    showfliers=True,
    palette=None,
    id_col="billing_account",
    min_n=1,
    show_counts=True,
    show_points=False,
    point_kwargs=None,
    rotate_xticks=True,
    display_counts_on_empty=False,
    boxplot_kwargs=None,
):
    """Plot original and clipped metric boxplots, optionally grouped by hue."""
    metrics = list(metrics)
    plot_source = data.copy()
    group_counts = pd.Series(dtype="int64")
    resolved_group_order = []

    if group_col is not None:
        plot_source = plot_source.dropna(subset=[group_col])
        if id_col in plot_source.columns:
            group_counts = __get_group_counts(plot_source, group_col, id_col=id_col)
        else:
            group_counts = plot_source.groupby(group_col, dropna=True).size()

        resolved_group_order = __resolve_group_order(
            group_counts,
            order=group_order,
            min_n=min_n,
        )
        plot_source = plot_source[plot_source[group_col].isin(resolved_group_order)]
        if plot_source.empty or not resolved_group_order:
            print("No groups meet the minimum sample size.")
            if display_counts_on_empty:
                display(
                    group_counts.rename("users")
                    .reset_index()
                    .sort_values("users", ascending=False)
                )
            return None

    metric_values = plot_source[metrics]
    lower_bounds = metric_values.quantile(lower_q)
    upper_bounds = metric_values.quantile(upper_q)
    clipped_values = metric_values.clip(lower=lower_bounds, upper=upper_bounds, axis=1)

    if group_col is not None:
        metric_values = metric_values.assign(**{group_col: plot_source[group_col].values})
        clipped_values = clipped_values.assign(**{group_col: plot_source[group_col].values})
        id_vars = [group_col]
    else:
        id_vars = None

    full_plot = metric_values.melt(
        id_vars=id_vars,
        value_vars=metrics,
        var_name="metric",
        value_name="value",
    ).dropna(subset=["value"])
    clipped_plot = clipped_values.melt(
        id_vars=id_vars,
        value_vars=metrics,
        var_name="metric",
        value_name="value",
    ).dropna(subset=["value"])

    if full_plot.empty or clipped_plot.empty:
        print("No non-null metric values available to plot.")
        return None

    for plot_df in [full_plot, clipped_plot]:
        plot_df["metric"] = pd.Categorical(
            plot_df["metric"],
            categories=metrics,
            ordered=True,
        )

    dataset_label = f" | {dataset_name} users" if dataset_name is not None else ""
    clip_label = f"{lower_q:.0%}-{upper_q:.0%}"
    group_title = f" by {group_col}" if group_col is not None else ""
    title = chart_title or f"Metric boxplots{group_title}{dataset_label}"

    fig, axes = plt.subplots(1, 2, figsize=figsize)

    resolved_boxplot_kwargs = {
        "x": "metric",
        "y": "value",
        "showfliers": showfliers,
    }
    if boxplot_kwargs is not None:
        resolved_boxplot_kwargs.update(boxplot_kwargs)
    if group_col is not None:
        resolved_boxplot_kwargs.update(
            hue=group_col,
            hue_order=resolved_group_order,
            palette=palette,
        )

    sns.boxplot(data=full_plot, ax=axes[0], **resolved_boxplot_kwargs)
    axes[0].set_title(f"Original Values{dataset_label}")
    axes[0].set_xlabel("Metric")
    axes[0].set_ylabel("Value")

    sns.boxplot(data=clipped_plot, ax=axes[1], **resolved_boxplot_kwargs)
    axes[1].set_title(f"Clipped Values ({clip_label}){dataset_label}")
    axes[1].set_xlabel("Metric")
    axes[1].set_ylabel("Value")
    fig.suptitle(title)

    if show_points:
        resolved_point_kwargs = {
            "alpha": 0.25,
            "size": 3,
            "jitter": 0.2,
        }
        if group_col is not None:
            resolved_point_kwargs.update(
                hue=group_col,
                hue_order=resolved_group_order,
                palette=palette,
                dodge=True,
                legend=False,
            )
        else:
            resolved_point_kwargs["color"] = "black"
        if point_kwargs is not None:
            resolved_point_kwargs.update(point_kwargs)

        sns.stripplot(
            data=full_plot,
            x="metric",
            y="value",
            ax=axes[0],
            **resolved_point_kwargs,
        )
        sns.stripplot(
            data=clipped_plot,
            x="metric",
            y="value",
            ax=axes[1],
            **resolved_point_kwargs,
        )

    if rotate_xticks:
        for ax in axes:
            plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    if group_col is not None:
        left_legend = axes[0].get_legend()
        if left_legend is not None:
            left_legend.remove()

        right_legend = axes[1].get_legend()
        if right_legend is not None:
            right_legend.set_title(group_col)
            if show_counts:
                label_map = {
                    str(group_value): f"{group_value} (n={group_counts.loc[group_value]:,})"
                    for group_value in resolved_group_order
                }
                for text in right_legend.get_texts():
                    text.set_text(label_map.get(text.get_text(), text.get_text()))

    __finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        chart_title=title,
        close=close,
        save_kwargs=save_kwargs,
    )
    return axes


def build_segment_combo_counts(
    data,
    metrics,
    slice_fields,
    group_col="Treatment",
    action_status="No Action yet",
    id_col="billing_account",
    min_n=5,
    treatment_order=ORDERS["Treatment"],
    show=False,
    save=True,
    chart_folder="charts",
    file_name_template=None,
    close=None,
    save_kwargs=None,
):
    """Count treatment users by segment combinations and plot each available slice."""
    action_data = data[data["status"].ne(action_status)].copy()
    segment_combo_counts = (action_data
        .groupby(slice_fields + [group_col], dropna=False)
        .agg(users=(id_col, "count"))
        .reset_index()
    )
    display(segment_combo_counts.head(5))

    plot_combo = segment_combo_counts[slice_fields].drop_duplicates()
    print(f"Plotting {len(plot_combo)} unique segment combinations...")
    for combo_index, row in enumerate(plot_combo.itertuples(index=False), start=1):
        filters = row._asdict()

        plot_df = __apply_segment_filters(action_data, filters)
        title_filters = ", ".join(
            f"{value}" for _, value in filters.items() if pd.notna(value)
        ) or "All action users"
        file_name = __format_file_name_template(
            file_name_template,
            combo_index=combo_index,
            segment_combo=title_filters,
            **filters,
        )
        plot_full_and_clipped_boxplot(
            data=plot_df,
            metrics=metrics,
            dataset_name=title_filters,
            group_col=group_col,
            group_order=treatment_order,
            min_n=min_n,
            show=show,
            save=save,
            chart_folder=chart_folder,
            file_name=file_name,
            chart_title=f"{title_filters}\n\nMetric boxplots by {group_col}",
            close=close,
            save_kwargs=save_kwargs,
        )
    return segment_combo_counts
