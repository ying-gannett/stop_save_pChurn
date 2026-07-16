"""Project-specific data summaries and chart builders for the EDA notebooks."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


_PERCENTILES = (0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99)
_TREATMENT_ORDER = ["Control", "Midpoint", "Tiered"]
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


def save_chart(fig, folder, file_name):
    """Save a matplotlib figure to the requested project output path."""
    output_folder = Path(folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    output_path = output_folder / file_name
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    return output_path


def _finalize_chart(
    fig,
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    close=None,
):
    """Apply layout, optionally save/show a chart, and close it when appropriate."""
    fig.tight_layout()

    saved_path = None
    if save:
        saved_path = save_chart(
            fig,
            folder=chart_folder,
            file_name=file_name,
        )

    if show:
        plt.show()

    should_close = not show if close is None else close
    if should_close:
        plt.close(fig)

    return saved_path


def cast_numeric_fields(data, numeric_fields):
    """Cast selected columns to float64 in place and return the dataframe."""
    for field in numeric_fields:
        data[field] = data[field].astype("float64")
    return data


def build_distribution_summary(data, numeric_fields):
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
    percentile_summary = data[numeric_fields].quantile(_PERCENTILES).T
    percentile_summary.columns = [f"p{int(p * 100):02d}" for p in _PERCENTILES]

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


def build_segment_summary(data, segment, metrics):
    """Aggregate user counts and numeric metric summaries by one segment."""
    agg_spec = {"users": ("billing_account", "nunique")}
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


def _fit_global_metric_reference(
    data,
    metrics,
):
    """Fit 1%-99% clipping bounds and robust scaling parameters per metric."""
    metric_values = data[metrics]
    lower_bounds = metric_values.quantile(0.01)
    upper_bounds = metric_values.quantile(0.99)
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
    min_n=1,
    metric_reference=None,
):
    """Prepare full, clipped, and standardized-clipped boxplot data."""
    plot_source = data.copy()
    group_counts = pd.Series(dtype="int64")
    resolved_group_order = []

    if group_col is not None:
        plot_source = plot_source.dropna(subset=[group_col])
        group_counts = plot_source.groupby(group_col)["billing_account"].nunique()
        valid_groups = group_counts[group_counts >= min_n].index.tolist()
        if group_order is None:
            resolved_group_order = (
                group_counts[group_counts >= min_n]
                .sort_values(ascending=False)
                .index.tolist()
            )
        else:
            resolved_group_order = [
                group for group in group_order if group in valid_groups
            ]
            resolved_group_order += [
                group for group in valid_groups if group not in resolved_group_order
            ]
        plot_source = plot_source[plot_source[group_col].isin(resolved_group_order)]
        if plot_source.empty or not resolved_group_order:
            return None

    full_values = plot_source[metrics].astype("float64")
    if metric_reference is None:
        metric_reference = _fit_global_metric_reference(full_values, metrics)

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
        close=close,
    )


def plot_metric_boxplot_views(
    data,
    metrics,
    group_col=None,
    group_order=None,
    figsize=(15, 4),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    showfliers=True,
    palette=None,
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
    group_title = f" by {group_col}" if group_col is not None else ""
    title = chart_title or f"Metric boxplots{group_title}"

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
        "Original Values",
        **common_plot_kwargs,
    )
    _plot_metric_boxplot_axis(
        axes[1],
        clipped_plot,
        "Clipped Values (1%-99%)",
        **common_plot_kwargs,
    )
    _plot_metric_boxplot_axis(
        axes[2],
        standardized_clipped_plot,
        "Standardized Clipped Values (1%-99%)",
        value_label="Value relative to global median (IQR units)",
        **common_plot_kwargs,
    )
    fig.suptitle(title)

    if group_col is not None:
        for ax in axes[:-1]:
            legend = ax.get_legend()
            if legend is not None:
                legend.remove()
        _format_metric_boxplot_legend(
            axes[-1].get_legend(),
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
        close=close,
    )
    return axes


def _prepare_segment_boxplot_panels(
    data,
    metrics,
    slice_fields,
    group_col,
    group_order,
    min_n,
):
    """Prepare counts and plot-ready data for every segment combination."""
    metric_reference = _fit_global_metric_reference(data, metrics)
    groupby_fields = [*slice_fields]
    if group_col is not None:
        groupby_fields.append(group_col)

    segment_combo_counts = (
        data.groupby(groupby_fields, dropna=False)
        .agg(users=("billing_account", "nunique"))
        .reset_index()
    )
    plot_combinations = segment_combo_counts[slice_fields].drop_duplicates()

    panels = []
    for row in plot_combinations.itertuples(index=False):
        filters = row._asdict()
        plot_data = data
        for column, value in filters.items():
            if pd.isna(value):
                plot_data = plot_data[plot_data[column].isna()]
            else:
                plot_data = plot_data[plot_data[column].eq(value)]
        title = ", ".join(
            f"{column}=Missing" if pd.isna(value) else str(value)
            for column, value in filters.items()
        ) or "All users"
        prepared = _prepare_metric_boxplot_data(
            data=plot_data,
            metrics=metrics,
            group_col=group_col,
            group_order=group_order,
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
            }
        )

    return segment_combo_counts, panels


def _render_segment_boxplot_pages(
    panels,
    plot_specs,
    panel_size,
    chart_folder,
    show,
    save,
    close,
    plot_kwargs,
):
    """Render every metric view with stable y-limits across all pages."""
    if not panels:
        return

    slices_per_file = 6
    n_cols = 2
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
            file_path = Path(spec["file_name"])
            page_file_name = spec["file_name"]
            if total_pages > 1:
                page_file_name = f"{file_path.stem}_{page_number}{file_path.suffix}"
            _plot_metric_boxplot_panels(
                panels=page_panels,
                plot_key=spec["plot_key"],
                figure_title=f"{spec['title']}{page_label}",
                n_cols=n_cols,
                panel_size=panel_size,
                show=show,
                save=save,
                chart_folder=chart_folder,
                file_name=page_file_name,
                close=close,
                y_limits=y_limits_by_plot[spec["plot_key"]],
                value_label=spec["value_label"],
                **plot_kwargs,
            )


def plot_slices_of_segments_boxplot(
    data,
    metrics,
    slice_fields,
    group_col="Treatment",
    min_n=5,
    group_order=None,
    show=False,
    save=True,
    chart_folder="charts",
    close=None,
    full_file_name="segment_slices_full.png",
    clipped_file_name="segment_slices_clipped.png",
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
    if group_order is None and group_col == "Treatment":
        group_order = _TREATMENT_ORDER

    segment_combo_counts, panels = _prepare_segment_boxplot_panels(
        data=data,
        metrics=metrics,
        slice_fields=slice_fields,
        group_col=group_col,
        group_order=group_order,
        min_n=min_n,
    )
    group_title = f" by {group_col}" if group_col is not None else ""

    plot_specs = [
        {
            "plot_key": "full_plot",
            "file_name": full_file_name,
            "title": f"Segment slice metric boxplots{group_title} | Full values",
            "value_label": "Value",
        },
        {
            "plot_key": "clipped_plot",
            "file_name": clipped_file_name,
            "title": (
                f"Segment slice metric boxplots{group_title} | "
                "Clipped values (1%-99%)"
            ),
            "value_label": "Value",
        },
        {
            "plot_key": "standardized_clipped_plot",
            "file_name": standardized_clipped_file_name,
            "title": (
                f"Segment slice metric boxplots{group_title} | "
                "Standardized clipped values (1%-99%)"
            ),
            "value_label": "Value relative to global median (IQR units)",
        },
    ]
    _render_segment_boxplot_pages(
        panels=panels,
        plot_specs=plot_specs,
        panel_size=panel_size,
        chart_folder=chart_folder,
        show=show,
        save=save,
        close=close,
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

    return segment_combo_counts
