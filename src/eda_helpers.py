import re
from pathlib import Path

from IPython.display import display
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from typing import Any, Callable


DEFAULT_PERCENTILES = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
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
        if pd.isna(value):
            filtered = filtered[filtered[col].isna()]
        else:
            filtered = filtered[filtered[col].eq(value)]

    return filtered


def __fit_global_robust_metric_scale(
    data,
    metrics,
    lower_q=0.01,
    upper_q=0.99,
):
    """Fit per-metric robust scaling parameters from one reference population."""
    if not 0 <= lower_q < upper_q <= 1:
        raise ValueError("lower_q and upper_q must satisfy 0 <= lower_q < upper_q <= 1.")

    metric_values = data[list(metrics)]
    lower_bounds = metric_values.quantile(lower_q)
    upper_bounds = metric_values.quantile(upper_q)
    centers = metric_values.median()
    spreads = metric_values.quantile(0.75) - metric_values.quantile(0.25)

    # A constant or highly discrete metric can have a zero IQR. Use its clipped
    # range as a fallback, and finally one to keep constant metrics well-defined.
    fallback_spreads = upper_bounds - lower_bounds
    spreads = spreads.mask(spreads.eq(0), fallback_spreads)
    spreads = spreads.mask(spreads.eq(0), 1.0).fillna(1.0)

    return {
        "lower_bounds": lower_bounds,
        "upper_bounds": upper_bounds,
        "centers": centers,
        "spreads": spreads,
    }


def __prepare_metric_boxplot_data(
    data,
    metrics,
    group_col=None,
    group_order=None,
    lower_q=0.01,
    upper_q=0.99,
    id_col="billing_account",
    min_n=1,
    display_counts_on_empty=False,
    metric_scale_params=None,
):
    """Prepare original and clipped long-form dataframes for metric boxplots."""
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

    raw_metric_values = plot_source[metrics].astype("float64")
    if metric_scale_params is None:
        lower_bounds = raw_metric_values.quantile(lower_q)
        upper_bounds = raw_metric_values.quantile(upper_q)
        metric_values = raw_metric_values
    else:
        lower_bounds = metric_scale_params["lower_bounds"]
        upper_bounds = metric_scale_params["upper_bounds"]
        metric_values = raw_metric_values.sub(
            metric_scale_params["centers"],
            axis=1,
        ).div(metric_scale_params["spreads"], axis=1)

    clipped_values = raw_metric_values.clip(
        lower=lower_bounds,
        upper=upper_bounds,
        axis=1,
    )
    if metric_scale_params is not None:
        clipped_values = clipped_values.sub(
            metric_scale_params["centers"],
            axis=1,
        ).div(metric_scale_params["spreads"], axis=1)

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

    return full_plot, clipped_plot, group_counts, resolved_group_order


def __plot_metric_boxplot_axis(
    ax,
    plot_df,
    title,
    group_col=None,
    group_order=None,
    showfliers=True,
    palette=None,
    show_points=False,
    point_kwargs=None,
    rotate_xticks=True,
    boxplot_kwargs=None,
    value_label="Value",
    y_limits=None,
):
    """Plot one metric boxplot axis with optional grouped hue and points."""
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
            hue_order=group_order,
            palette=palette,
        )

    sns.boxplot(data=plot_df, ax=ax, **resolved_boxplot_kwargs)
    ax.set_title(title)
    ax.set_xlabel("Metric")
    ax.set_ylabel(value_label)
    if y_limits is not None:
        ax.set_ylim(y_limits)

    if show_points:
        resolved_point_kwargs = {
            "alpha": 0.25,
            "size": 3,
            "jitter": 0.2,
        }
        if group_col is not None:
            resolved_point_kwargs.update(
                hue=group_col,
                hue_order=group_order,
                palette=palette,
                dodge=True,
                legend=False,
            )
        else:
            resolved_point_kwargs["color"] = "black"
        if point_kwargs is not None:
            resolved_point_kwargs.update(point_kwargs)

        sns.stripplot(
            data=plot_df,
            x="metric",
            y="value",
            ax=ax,
            **resolved_point_kwargs,
        )

    if rotate_xticks:
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    return ax


def __format_grouped_boxplot_legend(
    axes,
    group_col,
    group_counts,
    group_order,
    show_counts=True,
):
    """Keep one grouped boxplot legend and optionally append group counts."""
    if group_col is None:
        return

    left_legend = axes[0].get_legend()
    if left_legend is not None:
        left_legend.remove()

    right_legend = axes[1].get_legend()
    if right_legend is None:
        return

    __format_metric_boxplot_legend(
        right_legend,
        group_col,
        group_counts,
        group_order,
        show_counts=show_counts,
    )


def __format_metric_boxplot_legend(
    legend,
    group_col,
    group_counts,
    group_order,
    show_counts=True,
):
    """Format one grouped metric boxplot legend with optional group counts."""
    if legend is None or group_col is None:
        return

    legend.set_title(group_col)
    if not show_counts:
        return

    label_map = {
        str(group_value): f"{group_value} (n={group_counts.loc[group_value]:,})"
        for group_value in group_order
    }
    for text in legend.get_texts():
        text.set_text(label_map.get(text.get_text(), text.get_text()))


def __resolve_paginated_file_name(file_name, page_number, total_pages):
    """Append a page number to a file name when multiple pages are saved."""
    if total_pages <= 1:
        return file_name
    return __append_file_name_suffix(file_name, str(page_number))


def __get_metric_boxplot_value_limits(panels, plot_key, padding_fraction=0.05):
    """Return padded value limits shared by every panel and page of one plot type."""
    values = pd.concat(
        [panel[plot_key]["value"] for panel in panels],
        ignore_index=True,
    )
    finite_values = pd.to_numeric(values, errors="coerce")
    finite_values = finite_values[np.isfinite(finite_values)]
    if finite_values.empty:
        return None

    lower = finite_values.min()
    upper = finite_values.max()
    value_range = upper - lower
    if value_range == 0:
        padding = max(abs(lower) * padding_fraction, 0.5)
    else:
        padding = value_range * padding_fraction
    return lower - padding, upper + padding


def __plot_metric_boxplot_panels(
    panels,
    plot_key,
    figure_title,
    group_col=None,
    n_cols=2,
    panel_size=(7, 4),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    close=None,
    save_kwargs=None,
    showfliers=True,
    palette=None,
    show_counts=True,
    show_points=True,
    point_kwargs=None,
    rotate_xticks=True,
    boxplot_kwargs=None,
    sharey=False,
    value_label="Value",
    y_limits=None,
):
    """Plot one page of segment-slice metric boxplot panels."""
    if not panels:
        return None

    n_cols = max(1, min(n_cols, len(panels)))
    n_rows = (len(panels) + n_cols - 1) // n_cols
    figsize = (panel_size[0] * n_cols, panel_size[1] * n_rows)
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=figsize,
        squeeze=False,
        sharey=sharey,
    )
    flat_axes = axes.ravel()

    for ax, panel in zip(flat_axes, panels):
        __plot_metric_boxplot_axis(
            ax,
            panel[plot_key],
            panel["title"],
            group_col=group_col,
            group_order=panel["group_order"],
            showfliers=showfliers,
            palette=palette,
            show_points=show_points,
            point_kwargs=point_kwargs,
            rotate_xticks=rotate_xticks,
            boxplot_kwargs=boxplot_kwargs,
            value_label=value_label,
            y_limits=y_limits,
        )
        __format_metric_boxplot_legend(
            ax.get_legend(),
            group_col,
            panel["group_counts"],
            panel["group_order"],
            show_counts=show_counts,
        )

    for ax in flat_axes[len(panels):]:
        ax.set_visible(False)

    fig.suptitle(figure_title)
    return __finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        chart_title=figure_title,
        close=close,
        save_kwargs=save_kwargs,
    )


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
    show_points=True,
    point_kwargs=None,
    rotate_xticks=True,
    display_counts_on_empty=False,
    boxplot_kwargs=None,
):
    """Plot original and clipped metric boxplots, optionally grouped by hue."""
    prepared = __prepare_metric_boxplot_data(
        data=data,
        metrics=metrics,
        group_col=group_col,
        group_order=group_order,
        lower_q=lower_q,
        upper_q=upper_q,
        id_col=id_col,
        min_n=min_n,
        display_counts_on_empty=display_counts_on_empty,
    )
    if prepared is None:
        return None

    full_plot, clipped_plot, group_counts, resolved_group_order = prepared
    dataset_label = f" | {dataset_name} users" if dataset_name is not None else ""
    clip_label = f"{lower_q:.0%}-{upper_q:.0%}"
    group_title = f" by {group_col}" if group_col is not None else ""
    title = chart_title or f"Metric boxplots{group_title}{dataset_label}"

    fig, axes = plt.subplots(1, 2, figsize=figsize)
    common_plot_kwargs = {
        "group_col": group_col,
        "group_order": resolved_group_order,
        "showfliers": showfliers,
        "palette": palette,
        "show_points": show_points,
        "point_kwargs": point_kwargs,
        "rotate_xticks": rotate_xticks,
        "boxplot_kwargs": boxplot_kwargs,
    }
    __plot_metric_boxplot_axis(
        axes[0],
        full_plot,
        f"Original Values{dataset_label}",
        **common_plot_kwargs,
    )
    __plot_metric_boxplot_axis(
        axes[1],
        clipped_plot,
        f"Clipped Values ({clip_label}){dataset_label}",
        **common_plot_kwargs,
    )
    fig.suptitle(title)

    __format_grouped_boxplot_legend(
        axes,
        group_col,
        group_counts,
        resolved_group_order,
        show_counts=show_counts,
    )

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


def plot_slices_of_segments_boxplot(
    data,
    metrics,
    slice_fields,
    group_col="Treatment",
    action_status="No Action yet",
    id_col="billing_account",
    min_n=5,
    group_order=None,
    lower_q=0.01,
    upper_q=0.99,
    show=False,
    save=True,
    chart_folder="charts",
    close=None,
    save_kwargs=None,
    full_file_name="segment_slices_full.png",
    clipped_file_name="segment_slices_clipped.png",
    slices_per_file=6,
    n_cols=2,
    panel_size=(7, 4),
    showfliers=True,
    palette=None,
    show_counts=True,
    show_points=True,
    point_kwargs=None,
    rotate_xticks=True,
    display_counts_on_empty=False,
    boxplot_kwargs=None,
    metric_scale="raw",
    sharey=None,
):
    """Plot segment slice boxplots as paginated full and clipped chart files.

    Set ``metric_scale="robust_global"`` to center every metric on its global
    median and scale it by its global IQR. The same reference population and
    clipping bounds are then used for every segment and treatment.
    """
    if slices_per_file < 1:
        raise ValueError("slices_per_file must be at least 1.")
    supported_metric_scales = {"raw", "robust_global"}
    if metric_scale not in supported_metric_scales:
        supported = ", ".join(sorted(supported_metric_scales))
        raise ValueError(f"metric_scale must be one of: {supported}.")

    metrics = list(metrics)
    slice_fields = list(slice_fields)
    resolved_sharey = (
        metric_scale == "robust_global"
        if sharey is None
        else bool(sharey)
    )

    if group_order is None and group_col == "Treatment":
        group_order = ORDERS["Treatment"]

    action_data = data[data["status"].ne(action_status)].copy()
    metric_scale_params = None
    if metric_scale == "robust_global":
        metric_scale_params = __fit_global_robust_metric_scale(
            action_data,
            metrics,
            lower_q=lower_q,
            upper_q=upper_q,
        )

    groupby_fields = list(slice_fields)
    if group_col is not None:
        groupby_fields += [group_col]

    segment_combo_counts = (action_data
        .groupby(groupby_fields, dropna=False)
        .agg(users=(id_col, "count"))
        .reset_index()
    )
    display(segment_combo_counts.head(5))

    plot_combo = segment_combo_counts[slice_fields].drop_duplicates()
    print(f"Preparing {len(plot_combo)} unique segment combinations...")
    panels = []
    for combo_index, row in enumerate(plot_combo.itertuples(index=False), start=1):
        filters = row._asdict()

        plot_df = __apply_segment_filters(action_data, filters)
        title_filters = ", ".join(
            f"{col}=Missing" if pd.isna(value) else f"{value}"
            for col, value in filters.items()
        ) or "All action users"
        prepared = __prepare_metric_boxplot_data(
            data=plot_df,
            metrics=metrics,
            group_col=group_col,
            group_order=group_order,
            lower_q=lower_q,
            upper_q=upper_q,
            id_col=id_col,
            min_n=min_n,
            display_counts_on_empty=display_counts_on_empty,
            metric_scale_params=metric_scale_params,
        )
        if prepared is None:
            continue

        full_plot, clipped_plot, group_counts, resolved_group_order = prepared
        panels.append(
            {
                "title": title_filters,
                "full_plot": full_plot,
                "clipped_plot": clipped_plot,
                "group_counts": group_counts,
                "group_order": resolved_group_order,
                "combo_index": combo_index,
                "filters": filters,
            }
        )

    if not panels:
        print("No segment combinations available to plot.")
        segment_combo_counts.attrs["saved_paths"] = {"full": [], "clipped": []}
        segment_combo_counts.attrs["metric_scale"] = metric_scale
        return segment_combo_counts

    total_pages = (len(panels) + slices_per_file - 1) // slices_per_file
    saved_paths = {"full": [], "clipped": []}
    clip_label = f"{lower_q:.0%}-{upper_q:.0%}"
    group_title = f" by {group_col}" if group_col is not None else ""
    if metric_scale == "robust_global":
        value_label = "Value relative to global median (IQR units)"
        full_value_title = "Standardized full values"
        clipped_value_title = f"Standardized clipped values ({clip_label})"
    else:
        value_label = "Value"
        full_value_title = "Full values"
        clipped_value_title = f"Clipped values ({clip_label})"

    y_limits_by_plot = {"full_plot": None, "clipped_plot": None}
    if resolved_sharey:
        y_limits_by_plot = {
            plot_key: __get_metric_boxplot_value_limits(panels, plot_key)
            for plot_key in y_limits_by_plot
        }

    common_panel_kwargs = {
        "group_col": group_col,
        "n_cols": n_cols,
        "panel_size": panel_size,
        "show": show,
        "save": save,
        "chart_folder": chart_folder,
        "close": close,
        "save_kwargs": save_kwargs,
        "showfliers": showfliers,
        "palette": palette,
        "show_counts": show_counts,
        "show_points": show_points,
        "point_kwargs": point_kwargs,
        "rotate_xticks": rotate_xticks,
        "boxplot_kwargs": boxplot_kwargs,
        "sharey": resolved_sharey,
        "value_label": value_label,
    }

    for page_index in range(total_pages):
        page_number = page_index + 1
        page_panels = panels[
            page_index * slices_per_file:(page_index + 1) * slices_per_file
        ]
        page_label = f" page {page_number}/{total_pages}" if total_pages > 1 else ""
        plot_specs = [
            (
                "full",
                "full_plot",
                full_file_name,
                (
                    f"Segment slice metric boxplots{group_title} | "
                    f"{full_value_title}{page_label}"
                ),
            ),
            (
                "clipped",
                "clipped_plot",
                clipped_file_name,
                (
                    f"Segment slice metric boxplots{group_title} | "
                    f"{clipped_value_title}{page_label}"
                ),
            ),
        ]

        for plot_type, plot_key, base_file_name, figure_title in plot_specs:
            saved_path = __plot_metric_boxplot_panels(
                panels=page_panels,
                plot_key=plot_key,
                figure_title=figure_title,
                file_name=__resolve_paginated_file_name(
                    base_file_name,
                    page_number,
                    total_pages,
                ),
                y_limits=y_limits_by_plot[plot_key],
                **common_panel_kwargs,
            )
            if saved_path is not None:
                saved_paths[plot_type].append(str(saved_path))

    segment_combo_counts.attrs["saved_paths"] = saved_paths
    segment_combo_counts.attrs["metric_scale"] = metric_scale
    return segment_combo_counts
