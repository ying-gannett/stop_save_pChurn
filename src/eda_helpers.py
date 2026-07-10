"""Reusable data summaries and chart builders for the project EDA."""

import re
from pathlib import Path
from typing import Any, Callable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


DEFAULT_PERCENTILES = (0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99)
ORDERS = {
    "src_risk_tier": [
        "1. Low risk",
        "2. Med-Low risk",
        "3. Medium risk",
        "4. Med-High risk",
        "5. High risk",
    ],
    "cohort": ["Two-Offer Cohort", "Three-Offer Cohort"],
    "Treatment": ["Control", "Midpoint", "Tiered"],
    "contact_channel": [
        "No Action yet",
        "Called-In Cancel Flow",
        "Online Cancel Flow",
        "Called-In first",
        "Online first",
    ],
    "status": ["No Action yet", "saved", "stoped"],
    "contact_timing": ["Contact Before Pricing", "Contact On/After Pricing"],
    "repeatedly_called": [0, 1],
}
SEMANTIC_GROUP_COLORS = {
    "treatment": {
        "control": "#2E8B57",
        "midpoint": "#4C78A8",
        "tiered": "#8E5EA2",
    },
    "status": {
        "no action yet": "#8C8C8C",
        "saved": "#2E8B57",
        "stoped": "#C44E52",
        "stopped": "#C44E52",
    },
}

# Chart output helpers


def _slugify_file_name(value, max_length=120):
    """Convert a title or file name to a filesystem-friendly stem."""
    value = str(value).strip()
    value = re.sub(r"[^\w\s.-]", "", value)
    value = re.sub(r"[\s.-]+", "_", value)
    value = value.strip("_")
    return (value or "chart")[:max_length].rstrip("_")


def _resolve_chart_path(folder, file_name, extension):
    """Build the output path for a chart file."""
    extension = extension.lstrip(".")
    file_path = Path(file_name)
    suffix = file_path.suffix or f".{extension}"
    stem = _slugify_file_name(file_path.stem if file_path.suffix else file_path.name)
    return Path(folder) / f"{stem}{suffix}"


def _get_unique_path(path):
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
    output_path = _resolve_chart_path(output_folder, resolved_title, extension)
    if not overwrite:
        output_path = _get_unique_path(output_path)

    fig.savefig(output_path, dpi=dpi, bbox_inches=bbox_inches, **savefig_kwargs)
    return output_path


def _finalize_chart(
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


def _format_file_name_template(file_name, **values):
    """Render a file name template with known values when placeholders are present."""
    if file_name is None:
        return None

    try:
        return str(file_name).format(**values)
    except (KeyError, IndexError):
        return file_name


def _append_file_name_suffix(file_name, suffix):
    """Append a slugified suffix to a file name before its extension."""
    path = Path(file_name)
    suffix = _slugify_file_name(suffix)
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
        values = data[field].dropna()
        non_null_count = len(values)
        q1 = values.quantile(0.25)
        q3 = values.quantile(0.75)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outlier_count = ((values < lower_bound) | (values > upper_bound)).sum()
        outlier_pct = (
            outlier_count / non_null_count * 100
            if non_null_count
            else np.nan
        )

        outlier_summary.append({
            "field": field,
            "row_count": len(data),
            "non_null_count": non_null_count,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "outlier_count": outlier_count,
            "outlier_pct": outlier_pct,
        })

    return pd.DataFrame(outlier_summary)


def build_segment_summary(data, segment, metrics, id_col="billing_account"):
    """Aggregate user counts and numeric metric summaries by one segment."""
    agg_spec: dict[str, tuple[str, str | Callable[[Any], Any]]] = {
        "users": (id_col, "nunique")
    }
    for metric in metrics:
        agg_spec[f"avg_{metric}"] = (metric, "mean")
        agg_spec[f"median_{metric}"] = (metric, "median")
        agg_spec[f"p90_{metric}"] = (metric, lambda x: x.quantile(0.90))

    return (
        data.groupby(segment, dropna=False)
        .agg(**agg_spec)
        .reset_index()
        .sort_values("users", ascending=False)
    )


def _get_group_counts(data, group_col, id_col="billing_account", dropna=True):
    """Count unique accounts per group for plotting filters and labels."""
    return data.groupby(group_col, dropna=dropna)[id_col].nunique()


def _resolve_group_order(counts, order=None, min_n=1):
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

    _finalize_chart(
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
        return None

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

    _finalize_chart(
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
        return []

    sample_df = data.sample(min(sample_size, len(data)), random_state=random_state)
    pairs = list(pairs)
    saved_paths = []
    plotted_count = 0

    for x_col, y_col in pairs:
        if x_col in sample_df.columns and y_col in sample_df.columns:
            plotted_count += 1
            title = _format_file_name_template(
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

            resolved_file_name = _format_file_name_template(
                file_name,
                x_col=x_col,
                y_col=y_col,
                pair=f"{y_col}_vs_{x_col}",
                index=plotted_count,
            )
            if file_name is not None and resolved_file_name == file_name and len(pairs) > 1:
                resolved_file_name = _append_file_name_suffix(
                    file_name,
                    f"{y_col}_vs_{x_col}",
                )
            elif file_name is None and chart_title is not None and len(pairs) > 1:
                resolved_file_name = _append_file_name_suffix(
                    title,
                    f"{y_col}_vs_{x_col}",
                )

            saved_path = _finalize_chart(
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

    saved_path = _finalize_chart(
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


def _apply_segment_filters(data, filters):
    """Filter a dataframe to rows matching a segment-combination dictionary."""
    filtered = data

    for col, value in filters.items():
        if pd.isna(value):
            filtered = filtered[filtered[col].isna()]
        else:
            filtered = filtered[filtered[col].eq(value)]

    return filtered


def _fit_global_metric_reference(
    data,
    metrics,
    lower_q=0.01,
    upper_q=0.99,
):
    """Fit global clipping bounds and robust scaling parameters per metric."""
    if not 0 <= lower_q < upper_q <= 1:
        raise ValueError(
            "lower_q and upper_q must satisfy 0 <= lower_q < upper_q <= 1."
        )

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


def _prepare_metric_boxplot_data(
    data,
    metrics,
    group_col=None,
    group_order=None,
    lower_q=0.01,
    upper_q=0.99,
    id_col="billing_account",
    min_n=1,
    metric_reference=None,
):
    """Prepare full, clipped, and standardized-clipped boxplot data."""
    metrics = list(metrics)
    plot_source = data.copy()
    group_counts = pd.Series(dtype="int64")
    resolved_group_order = []

    if group_col is not None:
        plot_source = plot_source.dropna(subset=[group_col])
        group_counts = _get_group_counts(plot_source, group_col, id_col=id_col)

        resolved_group_order = _resolve_group_order(
            group_counts,
            order=group_order,
            min_n=min_n,
        )
        plot_source = plot_source[plot_source[group_col].isin(resolved_group_order)]
        if plot_source.empty or not resolved_group_order:
            return None

    full_values = plot_source[metrics].astype("float64")
    if metric_reference is None:
        metric_reference = _fit_global_metric_reference(
            full_values,
            metrics,
            lower_q=lower_q,
            upper_q=upper_q,
        )

    lower_bounds = metric_reference["lower_bounds"]
    upper_bounds = metric_reference["upper_bounds"]
    clipped_values = full_values.clip(
        lower=lower_bounds,
        upper=upper_bounds,
        axis=1,
    )
    standardized_clipped_values = clipped_values.sub(
        metric_reference["centers"],
        axis=1,
    ).div(metric_reference["spreads"], axis=1)

    if group_col is not None:
        group_values = {group_col: plot_source[group_col].values}
        full_values = full_values.assign(**group_values)
        clipped_values = clipped_values.assign(**group_values)
        standardized_clipped_values = standardized_clipped_values.assign(
            **group_values
        )
        id_vars = [group_col]
    else:
        id_vars = None

    full_plot = full_values.melt(
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
    standardized_clipped_plot = standardized_clipped_values.melt(
        id_vars=id_vars,
        value_vars=metrics,
        var_name="metric",
        value_name="value",
    ).dropna(subset=["value"])

    if full_plot.empty or clipped_plot.empty or standardized_clipped_plot.empty:
        return None

    for plot_df in [full_plot, clipped_plot, standardized_clipped_plot]:
        plot_df["metric"] = pd.Categorical(
            plot_df["metric"],
            categories=metrics,
            ordered=True,
        )

    return (
        full_plot,
        clipped_plot,
        standardized_clipped_plot,
        group_counts,
        resolved_group_order,
    )


def _resolve_metric_group_palette(group_col, group_order, palette=None):
    """Resolve semantic group colors while preserving explicit overrides."""
    if palette is not None or group_col is None:
        return palette

    semantic_colors = SEMANTIC_GROUP_COLORS.get(str(group_col).strip().lower())
    if semantic_colors is None:
        return None

    group_order = list(group_order or [])
    resolved_palette = {}
    unresolved_groups = []
    for group_value in group_order:
        normalized_value = str(group_value).strip().lower()
        color = semantic_colors.get(normalized_value)
        if color is None:
            unresolved_groups.append(group_value)
        else:
            resolved_palette[group_value] = color

    fallback_colors = sns.color_palette("deep", n_colors=len(unresolved_groups))
    resolved_palette.update(zip(unresolved_groups, fallback_colors))
    return resolved_palette


def _plot_metric_boxplot_axis(
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
    resolved_palette = _resolve_metric_group_palette(
        group_col,
        group_order,
        palette=palette,
    )
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
            palette=resolved_palette,
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
                palette=resolved_palette,
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


def _format_grouped_boxplot_legend(
    axes,
    group_col,
    group_counts,
    group_order,
    show_counts=True,
):
    """Keep one grouped boxplot legend and optionally append group counts."""
    if group_col is None:
        return

    for ax in axes[:-1]:
        legend = ax.get_legend()
        if legend is not None:
            legend.remove()

    final_legend = axes[-1].get_legend()
    if final_legend is None:
        return

    _format_metric_boxplot_legend(
        final_legend,
        group_col,
        group_counts,
        group_order,
        show_counts=show_counts,
    )


def _format_metric_boxplot_legend(
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


def _resolve_paginated_file_name(file_name, page_number, total_pages):
    """Append a page number to a file name when multiple pages are saved."""
    if total_pages <= 1:
        return file_name
    return _append_file_name_suffix(file_name, str(page_number))


def _get_metric_boxplot_value_limits(panels, plot_key, padding_fraction=0.05):
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


def _plot_metric_boxplot_panels(
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
        sharey=True,
    )
    flat_axes = axes.ravel()

    for ax, panel in zip(flat_axes, panels):
        _plot_metric_boxplot_axis(
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
        _format_metric_boxplot_legend(
            ax.get_legend(),
            group_col,
            panel["group_counts"],
            panel["group_order"],
            show_counts=show_counts,
        )

    for ax in flat_axes[len(panels):]:
        ax.set_visible(False)

    fig.suptitle(figure_title)
    return _finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        chart_title=figure_title,
        close=close,
        save_kwargs=save_kwargs,
    )


def plot_metric_boxplot_views(
    data,
    metrics,
    dataset_name=None,
    group_col=None,
    group_order=None,
    lower_q=0.01,
    upper_q=0.99,
    figsize=(15, 4),
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
    boxplot_kwargs=None,
):
    """Plot full, clipped, and standardized-clipped metric boxplots."""
    prepared = _prepare_metric_boxplot_data(
        data=data,
        metrics=metrics,
        group_col=group_col,
        group_order=group_order,
        lower_q=lower_q,
        upper_q=upper_q,
        id_col=id_col,
        min_n=min_n,
    )
    if prepared is None:
        return None

    (
        full_plot,
        clipped_plot,
        standardized_clipped_plot,
        group_counts,
        resolved_group_order,
    ) = prepared
    dataset_label = f" | {dataset_name} users" if dataset_name is not None else ""
    clip_label = f"{lower_q:.0%}-{upper_q:.0%}"
    group_title = f" by {group_col}" if group_col is not None else ""
    title = chart_title or f"Metric boxplots{group_title}{dataset_label}"

    fig, axes = plt.subplots(1, 3, figsize=figsize)
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
    _plot_metric_boxplot_axis(
        axes[0],
        full_plot,
        f"Original Values{dataset_label}",
        **common_plot_kwargs,
    )
    _plot_metric_boxplot_axis(
        axes[1],
        clipped_plot,
        f"Clipped Values ({clip_label}){dataset_label}",
        **common_plot_kwargs,
    )
    _plot_metric_boxplot_axis(
        axes[2],
        standardized_clipped_plot,
        f"Standardized Clipped Values ({clip_label}){dataset_label}",
        value_label="Value relative to global median (IQR units)",
        **common_plot_kwargs,
    )
    fig.suptitle(title)

    _format_grouped_boxplot_legend(
        axes,
        group_col,
        group_counts,
        resolved_group_order,
        show_counts=show_counts,
    )

    _finalize_chart(
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


plot_full_and_clipped_boxplot = plot_metric_boxplot_views


def _prepare_segment_boxplot_panels(
    data,
    metrics,
    slice_fields,
    group_col,
    group_order,
    id_col,
    min_n,
    lower_q,
    upper_q,
):
    """Prepare counts and plot-ready data for every segment combination."""
    metric_reference = _fit_global_metric_reference(
        data,
        metrics,
        lower_q=lower_q,
        upper_q=upper_q,
    )
    groupby_fields = [*slice_fields]
    if group_col is not None:
        groupby_fields.append(group_col)

    segment_combo_counts = (
        data.groupby(groupby_fields, dropna=False)
        .agg(users=(id_col, "nunique"))
        .reset_index()
    )
    plot_combinations = segment_combo_counts[slice_fields].drop_duplicates()

    panels = []
    for combo_index, row in enumerate(
        plot_combinations.itertuples(index=False),
        start=1,
    ):
        filters = row._asdict()
        plot_data = _apply_segment_filters(data, filters)
        title = ", ".join(
            f"{column}=Missing" if pd.isna(value) else str(value)
            for column, value in filters.items()
        ) or "All users"
        prepared = _prepare_metric_boxplot_data(
            data=plot_data,
            metrics=metrics,
            group_col=group_col,
            group_order=group_order,
            lower_q=lower_q,
            upper_q=upper_q,
            id_col=id_col,
            min_n=min_n,
            metric_reference=metric_reference,
        )
        if prepared is None:
            continue

        (
            full_plot,
            clipped_plot,
            standardized_clipped_plot,
            group_counts,
            resolved_group_order,
        ) = prepared
        panels.append(
            {
                "title": title,
                "full_plot": full_plot,
                "clipped_plot": clipped_plot,
                "standardized_clipped_plot": standardized_clipped_plot,
                "group_counts": group_counts,
                "group_order": resolved_group_order,
                "combo_index": combo_index,
                "filters": filters,
            }
        )

    return segment_combo_counts, panels


def _render_segment_boxplot_pages(
    panels,
    plot_specs,
    slices_per_file,
    n_cols,
    panel_size,
    chart_folder,
    show,
    save,
    close,
    save_kwargs,
    plot_kwargs,
):
    """Render every metric view with stable y-limits across all pages."""
    saved_paths = {spec["name"]: [] for spec in plot_specs}
    if not panels:
        return saved_paths

    total_pages = (len(panels) + slices_per_file - 1) // slices_per_file
    y_limits_by_plot = {
        spec["plot_key"]: _get_metric_boxplot_value_limits(
            panels,
            spec["plot_key"],
        )
        for spec in plot_specs
    }

    for page_index in range(total_pages):
        page_number = page_index + 1
        page_panels = panels[
            page_index * slices_per_file:(page_index + 1) * slices_per_file
        ]
        page_label = f" page {page_number}/{total_pages}" if total_pages > 1 else ""

        for spec in plot_specs:
            saved_path = _plot_metric_boxplot_panels(
                panels=page_panels,
                plot_key=spec["plot_key"],
                figure_title=f"{spec['title']}{page_label}",
                n_cols=n_cols,
                panel_size=panel_size,
                show=show,
                save=save,
                chart_folder=chart_folder,
                file_name=_resolve_paginated_file_name(
                    spec["file_name"],
                    page_number,
                    total_pages,
                ),
                close=close,
                save_kwargs=save_kwargs,
                y_limits=y_limits_by_plot[spec["plot_key"]],
                value_label=spec["value_label"],
                **plot_kwargs,
            )
            if saved_path is not None:
                saved_paths[spec["name"]].append(str(saved_path))

    return saved_paths


def plot_slices_of_segments_boxplot(
    data,
    metrics,
    slice_fields,
    group_col="Treatment",
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
    boxplot_kwargs=None,
    standardized_clipped_file_name="segment_slices_standardized_clipped.png",
):
    """Plot three paginated metric views for already-filtered segment data."""
    if slices_per_file < 1:
        raise ValueError("slices_per_file must be at least 1.")

    metrics = list(metrics)
    slice_fields = list(slice_fields)
    if not slice_fields:
        raise ValueError("slice_fields must contain at least one column.")

    if group_order is None and group_col == "Treatment":
        group_order = ORDERS["Treatment"]

    segment_combo_counts, panels = _prepare_segment_boxplot_panels(
        data=data,
        metrics=metrics,
        slice_fields=slice_fields,
        group_col=group_col,
        group_order=group_order,
        id_col=id_col,
        min_n=min_n,
        lower_q=lower_q,
        upper_q=upper_q,
    )
    clip_label = f"{lower_q:.0%}-{upper_q:.0%}"
    group_title = f" by {group_col}" if group_col is not None else ""

    plot_specs = [
        {
            "name": "full",
            "plot_key": "full_plot",
            "file_name": full_file_name,
            "title": f"Segment slice metric boxplots{group_title} | Full values",
            "value_label": "Value",
        },
        {
            "name": "clipped",
            "plot_key": "clipped_plot",
            "file_name": clipped_file_name,
            "title": (
                f"Segment slice metric boxplots{group_title} | "
                f"Clipped values ({clip_label})"
            ),
            "value_label": "Value",
        },
        {
            "name": "standardized_clipped",
            "plot_key": "standardized_clipped_plot",
            "file_name": standardized_clipped_file_name,
            "title": (
                f"Segment slice metric boxplots{group_title} | "
                f"Standardized clipped values ({clip_label})"
            ),
            "value_label": "Value relative to global median (IQR units)",
        },
    ]
    saved_paths = _render_segment_boxplot_pages(
        panels=panels,
        plot_specs=plot_specs,
        slices_per_file=slices_per_file,
        n_cols=n_cols,
        panel_size=panel_size,
        chart_folder=chart_folder,
        show=show,
        save=save,
        close=close,
        save_kwargs=save_kwargs,
        plot_kwargs={
            "group_col": group_col,
            "showfliers": showfliers,
            "palette": palette,
            "show_counts": show_counts,
            "show_points": show_points,
            "point_kwargs": point_kwargs,
            "rotate_xticks": rotate_xticks,
            "boxplot_kwargs": boxplot_kwargs,
        },
    )

    segment_combo_counts.attrs["saved_paths"] = saved_paths
    return segment_combo_counts
