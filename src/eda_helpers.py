"""Project-specific summaries and charts for the stop/save behavioral EDA."""

import textwrap
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


_PERCENTILES = (0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99)
_TREATMENT_ORDER = ["Control", "Midpoint", "Tiered"]
ID_COL = "billing_account"
OUTCOME_COL = "outcome"
SAVED = "Saved"
STOPPED = "Stopped"
OUTCOMES = (SAVED, STOPPED)
OUTCOME_COLORS = {
    SAVED: "#2E8B57",
    STOPPED: "#C44E52",
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
    "outcome": {
        "saved": OUTCOME_COLORS[SAVED],
        "stopped": OUTCOME_COLORS[STOPPED],
    },
    "repeated_call_group": {
        "called once": "#4C78A8",
        "repeatedly called": "#F28E2B",
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
    tight_layout_kwargs=None,
):
    """Apply layout, optionally save/show a chart, and close it when appropriate."""
    fig.tight_layout(**(tight_layout_kwargs or {}))

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


def fit_behavior_reference(
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
        metric_reference = fit_behavior_reference(full_values, metrics)

    clipped_values = _clip_metric_values(
        full_values,
        metrics,
        metric_reference,
    )
    standardized_clipped_values = transform_behavior_metrics(
        full_values,
        metrics,
        metric_reference,
    )

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
    metric_reference = fit_behavior_reference(data, metrics)
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


def _clip_metric_values(data, metrics, reference):
    """Clip metrics to the primary contacted-population bounds."""
    return data[metrics].clip(
        lower=reference["lower_bounds"][metrics],
        upper=reference["upper_bounds"][metrics],
        axis=1,
    )


def transform_behavior_metrics(data, metrics, reference):
    """Express clipped metrics in primary-population median/IQR units."""
    clipped = _clip_metric_values(data, metrics, reference)
    return clipped.sub(reference["centers"][metrics], axis=1).div(
        reference["spreads"][metrics],
        axis=1,
    )


def _segment_label(values, segment_fields):
    return " · ".join(
        "Missing" if pd.isna(values[field]) else str(values[field])
        for field in segment_fields
    )


def _vector_magnitude(values):
    finite = np.asarray(values, dtype="float64")
    finite = finite[np.isfinite(finite)]
    if not finite.size:
        return np.nan
    return float(np.sqrt(np.square(finite).sum()))


def build_behavior_profiles(
    data,
    metrics,
    segment_fields,
    reference,
    min_n=20,
):
    """Summarize absolute segment profiles against the primary reference."""
    working = data.reset_index(drop=True)
    scores = transform_behavior_metrics(working, metrics, reference)

    records = []
    grouped = working.groupby(
        segment_fields,
        dropna=False,
        observed=True,
        sort=False,
    )
    for key, group in grouped:
        if not isinstance(key, tuple):
            key = (key,)
        segment_values = dict(zip(segment_fields, key))
        users = group[ID_COL].nunique()
        if users < min_n:
            continue

        raw_medians = group[metrics].median()
        score_medians = scores.loc[group.index, metrics].median()
        finite_scores = score_medians.dropna()
        records.append(
            {
                **segment_values,
                "segment_label": _segment_label(segment_values, segment_fields),
                "users": users,
                **{f"median__{metric}": raw_medians[metric] for metric in metrics},
                **{f"score__{metric}": score_medians[metric] for metric in metrics},
                "profile_magnitude": _vector_magnitude(score_medians),
                "dominant_metric": (
                    finite_scores.abs().idxmax()
                    if not finite_scores.empty
                    else None
                ),
            }
        )

    if not records:
        raise ValueError(f"No behavior profiles meet min_n={min_n}.")
    return (
        pd.DataFrame.from_records(records)
        .sort_values(
            ["profile_magnitude", "users"],
            ascending=[False, False],
        )
        .reset_index(drop=True)
    )


def build_outcome_contrasts(
    data,
    metrics,
    segment_fields,
    reference,
    min_n=20,
):
    """Compare Saved with Stopped users in matched primary-population slices."""
    working = data.reset_index(drop=True)
    scores = transform_behavior_metrics(working, metrics, reference)

    records = []
    grouped = working.groupby(
        segment_fields,
        dropna=False,
        observed=True,
        sort=False,
    )
    for key, group in grouped:
        if not isinstance(key, tuple):
            key = (key,)
        segment_values = dict(zip(segment_fields, key))
        saved = group[group[OUTCOME_COL].eq(SAVED)]
        stopped = group[group[OUTCOME_COL].eq(STOPPED)]
        saved_n = saved[ID_COL].nunique()
        stopped_n = stopped[ID_COL].nunique()
        if saved_n < min_n or stopped_n < min_n:
            continue

        saved_medians = saved[metrics].median()
        stopped_medians = stopped[metrics].median()
        saved_scores = scores.loc[saved.index, metrics].median()
        stopped_scores = scores.loc[stopped.index, metrics].median()
        deltas = saved_scores - stopped_scores
        finite_deltas = deltas.dropna()
        dominant_metric = (
            finite_deltas.abs().idxmax() if not finite_deltas.empty else None
        )
        dominant_outcome = None
        if dominant_metric is not None:
            dominant_outcome = SAVED if deltas[dominant_metric] >= 0 else STOPPED
        total_users = saved_n + stopped_n
        records.append(
            {
                **segment_values,
                "segment_label": _segment_label(segment_values, segment_fields),
                "n__saved": saved_n,
                "n__stopped": stopped_n,
                **{
                    f"median_saved__{metric}": saved_medians[metric]
                    for metric in metrics
                },
                **{
                    f"median_stopped__{metric}": stopped_medians[metric]
                    for metric in metrics
                },
                **{f"delta__{metric}": deltas[metric] for metric in metrics},
                "total_users": total_users,
                "observed_saved_share": saved_n / total_users,
                "contrast_magnitude": _vector_magnitude(deltas),
                "dominant_metric": dominant_metric,
                "dominant_outcome": dominant_outcome,
            }
        )

    if not records:
        raise ValueError(
            "No matched outcome contrasts have at least "
            f"min_n={min_n} users in both outcomes."
        )
    return (
        pd.DataFrame.from_records(records)
        .sort_values(
            ["contrast_magnitude", "total_users"],
            ascending=[False, False],
        )
        .reset_index(drop=True)
    )


def plot_behavior_profile_heatmap(
    profiles,
    metrics,
    max_rows=30,
    color_limit=2.0,
    title="Behavior profiles relative to primary contacted users",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="behavior_profiles.png",
    close=None,
):
    """Plot absolute segment profile scores as a compact heatmap."""
    plot_data = profiles.head(max_rows)
    matrix = plot_data[[f"score__{metric}" for metric in metrics]].copy()
    matrix.columns = metrics
    matrix.index = [
        f"{label}  (n={users:,})"
        for label, users in zip(plot_data["segment_label"], plot_data["users"])
    ]
    limit = float(color_limit)
    fig_height = max(4.0, 0.42 * len(matrix) + 1.8)
    fig_width = max(8.0, 1.8 * len(metrics) + 5.5)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    sns.heatmap(
        matrix,
        ax=ax,
        cmap="vlag",
        center=0,
        vmin=-limit,
        vmax=limit,
        annot=True,
        fmt=".2f",
        linewidths=0.5,
        linecolor="white",
        cbar_kws={"label": "Median relative to primary population (IQR units)"},
    )
    ax.set_title(title)
    ax.set_xlabel("Behavior metric")
    ax.set_ylabel("Segment")
    ax.tick_params(axis="x", rotation=0)
    ax.tick_params(axis="y", rotation=0)
    saved_path = _finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        close=close,
    )
    ax._saved_path = Path(saved_path) if saved_path is not None else None
    return ax


def plot_outcome_contrast_heatmap(
    contrasts,
    metrics,
    max_rows=20,
    color_limit=1.5,
    title="Behavior contrast: Saved minus Stopped",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="saved_minus_stopped_profiles.png",
    close=None,
):
    """Plot within-slice outcome differences in common IQR units."""
    plot_data = contrasts.head(max_rows)
    matrix = plot_data[[f"delta__{metric}" for metric in metrics]].copy()
    matrix.columns = metrics
    matrix.index = [
        f"{label}  (n={n_a:,}/{n_b:,})"
        for label, n_a, n_b in zip(
            plot_data["segment_label"],
            plot_data["n__saved"],
            plot_data["n__stopped"],
        )
    ]
    limit = float(color_limit)
    fig_height = max(4.0, 0.48 * len(matrix) + 1.8)
    fig_width = max(8.0, 1.8 * len(metrics) + 5.5)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    sns.heatmap(
        matrix,
        ax=ax,
        cmap="vlag",
        center=0,
        vmin=-limit,
        vmax=limit,
        annot=True,
        fmt=".2f",
        linewidths=0.5,
        linecolor="white",
        cbar_kws={"label": "Saved minus Stopped median (IQR units)"},
    )
    ax.set_title(title)
    ax.set_xlabel("Behavior metric")
    ax.set_ylabel("Matched segment")
    ax.tick_params(axis="x", rotation=0)
    ax.tick_params(axis="y", rotation=0)
    saved_path = _finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        close=close,
    )
    ax._saved_path = Path(saved_path) if saved_path is not None else None
    return ax


def _match_segment(data, segment_values, segment_fields):
    mask = pd.Series(True, index=data.index)
    for field in segment_fields:
        value = segment_values[field]
        if pd.isna(value):
            mask &= data[field].isna()
        else:
            mask &= data[field].eq(value)
    return data[mask]


def _bootstrap_median_difference(
    first_values,
    second_values,
    iterations,
    confidence_level,
    rng,
):
    """Return a percentile bootstrap interval for an independent median difference."""
    first_values = np.asarray(first_values, dtype="float64")
    second_values = np.asarray(second_values, dtype="float64")
    first_values = first_values[np.isfinite(first_values)]
    second_values = second_values[np.isfinite(second_values)]
    if not first_values.size or not second_values.size or iterations < 1:
        return np.nan, np.nan

    max_sample_size = max(first_values.size, second_values.size)
    batch_size = max(1, min(iterations, 1_000_000 // max_sample_size))
    bootstrap_differences = np.empty(iterations, dtype="float64")
    for start in range(0, iterations, batch_size):
        stop = min(start + batch_size, iterations)
        current_batch_size = stop - start
        first_indices = rng.integers(
            0,
            first_values.size,
            size=(current_batch_size, first_values.size),
        )
        first_medians = np.median(first_values[first_indices], axis=1)
        del first_indices
        second_indices = rng.integers(
            0,
            second_values.size,
            size=(current_batch_size, second_values.size),
        )
        bootstrap_differences[start:stop] = first_medians - np.median(
            second_values[second_indices],
            axis=1,
        )
    tail_probability = (1 - confidence_level) / 2
    lower, upper = np.quantile(
        bootstrap_differences,
        [tail_probability, 1 - tail_probability],
    )
    return float(lower), float(upper)


def build_selected_segment_detail_table(
    data,
    contrasts,
    metrics,
    segment_fields,
    reference,
    top_n=8,
    bootstrap_iterations=0,
    confidence_level=0.95,
    random_state=42,
):
    """Build clipped spread and uncertainty details for selected contrasts."""
    selected = contrasts.head(top_n).reset_index(drop=True).copy()
    selected.insert(0, "segment_rank", np.arange(1, len(selected) + 1))
    rng = np.random.default_rng(random_state)
    records = []

    for _, selected_segment in selected.iterrows():
        segment_data = _match_segment(data, selected_segment, segment_fields)
        clipped_values = _clip_metric_values(segment_data, metrics, reference)
        base_record = {
            "segment_rank": selected_segment["segment_rank"],
            "segment_label": selected_segment["segment_label"],
            **{field: selected_segment[field] for field in segment_fields},
        }

        saved_mask = segment_data[OUTCOME_COL].eq(SAVED).to_numpy()
        stopped_mask = segment_data[OUTCOME_COL].eq(STOPPED).to_numpy()
        for metric in metrics:
            record = {**base_record, "metric": metric}
            metric_arrays = {}
            for token, mask in (
                ("saved", saved_mask),
                ("stopped", stopped_mask),
            ):
                metric_values = clipped_values.loc[mask, metric].dropna()
                metric_arrays[token] = metric_values.to_numpy()
                q25 = metric_values.quantile(0.25)
                q75 = metric_values.quantile(0.75)
                raw_median = selected_segment[f"median_{token}__{metric}"]
                clipped_median = (
                    float(
                        np.clip(
                            raw_median,
                            reference["lower_bounds"][metric],
                            reference["upper_bounds"][metric],
                        )
                    )
                    if pd.notna(raw_median)
                    else np.nan
                )
                record.update(
                    {
                        f"n__{token}": int(selected_segment[f"n__{token}"]),
                        f"non_null__{token}": int(metric_values.size),
                        f"clipped_median__{token}": clipped_median,
                        f"clipped_q25__{token}": q25,
                        f"clipped_q75__{token}": q75,
                        f"clipped_iqr__{token}": q75 - q25,
                    }
                )

            ci_lower, ci_upper = _bootstrap_median_difference(
                metric_arrays["saved"],
                metric_arrays["stopped"],
                bootstrap_iterations,
                confidence_level,
                rng,
            )
            record.update(
                clipped_median_difference=(
                    record["clipped_median__saved"]
                    - record["clipped_median__stopped"]
                ),
                clipped_median_difference_ci_lower=ci_lower,
                clipped_median_difference_ci_upper=ci_upper,
                bootstrap_iterations=bootstrap_iterations,
                confidence_level=confidence_level,
            )
            records.append(record)

    return pd.DataFrame.from_records(records)


def plot_selected_segment_clipped_boxplot_grid(
    data,
    contrasts,
    metrics,
    segment_fields,
    reference,
    top_n=8,
    panel_size=(3.0, 2.6),
    title="Raw clipped values for business magnitude",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="selected_segments_raw_clipped.png",
    close=None,
):
    """Plot clipped business-unit values for selected contrast rows."""
    selected = contrasts.head(top_n)
    n_rows = len(selected)
    n_cols = len(metrics)
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(panel_size[0] * n_cols, panel_size[1] * n_rows),
        sharey="col",
        squeeze=False,
    )

    for row_index, (_, selected_segment) in enumerate(selected.iterrows()):
        segment_data = _match_segment(data, selected_segment, segment_fields)
        metric_values = _clip_metric_values(segment_data, metrics, reference)

        for column_index, metric in enumerate(metrics):
            ax = axes[row_index, column_index]
            plot_data = pd.DataFrame(
                {
                    OUTCOME_COL: segment_data[OUTCOME_COL].to_numpy(),
                    "value": metric_values[metric].to_numpy(),
                }
            ).dropna(subset=["value"])
            sns.boxplot(
                data=plot_data,
                x=OUTCOME_COL,
                y="value",
                order=OUTCOMES,
                hue=OUTCOME_COL,
                hue_order=OUTCOMES,
                palette=OUTCOME_COLORS,
                showfliers=True,
                legend=False,
                ax=ax,
            )
            ax.set_xticks(
                range(len(OUTCOMES)),
                [
                    f"{SAVED}\nn={int(selected_segment['n__saved']):,}",
                    f"{STOPPED}\nn={int(selected_segment['n__stopped']):,}",
                ],
            )
            ax.set_xlabel("")
            if row_index == 0:
                ax.set_title(metric)
            if column_index == 0:
                ax.set_ylabel("Value")
                wrapped_label = textwrap.fill(
                    str(selected_segment["segment_label"]),
                    width=42,
                )
                ax.text(
                    -0.58,
                    0.5,
                    wrapped_label,
                    transform=ax.transAxes,
                    ha="right",
                    va="center",
                    fontsize=8.5,
                )
            else:
                ax.set_ylabel("")

    fig.suptitle(title, fontsize=15, y=0.995)
    saved_path = _finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        close=close,
        tight_layout_kwargs={"rect": (0.22, 0, 1, 0.98), "h_pad": 1.2},
    )
    visible_axes = list(axes.ravel())
    for ax in visible_axes:
        ax._saved_path = Path(saved_path) if saved_path is not None else None
    return visible_axes


def plot_top_behavior_contrasts(
    data,
    contrasts,
    metrics,
    segment_fields,
    reference,
    top_n=8,
    n_cols=2,
    panel_size=(7, 4),
    title="Top supported behavior contrasts: Saved vs Stopped",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="top_behavior_contrasts.png",
    close=None,
):
    """Drill into the strongest supported outcome contrasts with boxplots."""
    selected = contrasts.head(top_n)
    n_rows = int(np.ceil(len(selected) / n_cols))
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(panel_size[0] * n_cols, panel_size[1] * n_rows),
        sharey=True,
        squeeze=False,
    )
    flat_axes = axes.ravel()

    for panel_index, (_, contrast) in enumerate(selected.iterrows()):
        ax = flat_axes[panel_index]
        segment_data = _match_segment(data, contrast, segment_fields)
        scores = transform_behavior_metrics(segment_data, metrics, reference)
        long_data = scores.assign(
            **{OUTCOME_COL: segment_data[OUTCOME_COL].to_numpy()}
        ).melt(
            id_vars=[OUTCOME_COL],
            value_vars=metrics,
            var_name="metric",
            value_name="score",
        )
        sns.boxplot(
            data=long_data,
            x="metric",
            y="score",
            hue=OUTCOME_COL,
            hue_order=OUTCOMES,
            palette=OUTCOME_COLORS,
            showfliers=True,
            ax=ax,
        )
        counts = segment_data.groupby(OUTCOME_COL, observed=True)[ID_COL].nunique()
        count_text = " | ".join(
            f"{outcome} n={int(counts.get(outcome, 0)):,}"
            for outcome in OUTCOMES
        )
        ax.set_title(f"{contrast['segment_label']}\n{count_text}", fontsize=10)
        ax.axhline(0, color="#666666", linewidth=0.8, linestyle="--")
        ax.set_xlabel("Behavior metric")
        ax.set_ylabel("Primary-population median/IQR units")
        ax.tick_params(axis="x", rotation=30)
        if panel_index:
            legend = ax.get_legend()
            if legend is not None:
                legend.remove()
        else:
            ax.legend(title=OUTCOME_COL)

    for ax in flat_axes[len(selected):]:
        ax.set_visible(False)

    fig.suptitle(title, fontsize=15, y=1.01)
    saved_path = _finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        close=close,
    )
    visible_axes = [ax for ax in flat_axes if ax.get_visible()]
    for ax in visible_axes:
        ax._saved_path = Path(saved_path) if saved_path is not None else None
    return visible_axes


def build_selected_segment_treatment_contrasts(
    data,
    contrasts,
    metrics,
    segment_fields,
    reference,
    top_n=8,
    min_n=5,
):
    """Drill selected behavioral segments down to the assigned Treatments."""
    selected = contrasts.head(top_n).reset_index(drop=True).copy()
    selected.insert(0, "segment_rank", np.arange(1, len(selected) + 1))
    scores = transform_behavior_metrics(data, metrics, reference)
    records = []

    for _, selected_segment in selected.iterrows():
        segment_data = _match_segment(data, selected_segment, segment_fields)
        available_treatments = segment_data["Treatment"].dropna().unique().tolist()
        treatments = [
            treatment
            for treatment in _TREATMENT_ORDER
            if treatment in available_treatments
        ]
        treatments += [
            treatment
            for treatment in available_treatments
            if treatment not in treatments
        ]

        for treatment in treatments:
            treatment_data = segment_data[
                segment_data["Treatment"].eq(treatment)
            ]
            saved = treatment_data[treatment_data[OUTCOME_COL].eq(SAVED)]
            stopped = treatment_data[treatment_data[OUTCOME_COL].eq(STOPPED)]
            saved_n = saved[ID_COL].nunique()
            stopped_n = stopped[ID_COL].nunique()
            total_users = saved_n + stopped_n
            supported = saved_n >= min_n and stopped_n >= min_n

            saved_medians = saved[metrics].median()
            stopped_medians = stopped[metrics].median()
            saved_scores = scores.loc[saved.index, metrics].median()
            stopped_scores = scores.loc[stopped.index, metrics].median()
            deltas = saved_scores - stopped_scores
            if not supported:
                deltas[:] = np.nan

            finite_deltas = deltas.dropna()
            dominant_metric = (
                finite_deltas.abs().idxmax()
                if not finite_deltas.empty
                else None
            )
            records.append(
                {
                    "segment_rank": selected_segment["segment_rank"],
                    "segment_label": selected_segment["segment_label"],
                    **{
                        field: selected_segment[field]
                        for field in segment_fields
                    },
                    "Treatment": treatment,
                    "n__saved": saved_n,
                    "n__stopped": stopped_n,
                    "total_users": total_users,
                    "observed_saved_share": (
                        saved_n / total_users if total_users else np.nan
                    ),
                    "supported": supported,
                    **{
                        f"median_saved__{metric}": saved_medians[metric]
                        for metric in metrics
                    },
                    **{
                        f"median_stopped__{metric}": stopped_medians[metric]
                        for metric in metrics
                    },
                    **{f"delta__{metric}": deltas[metric] for metric in metrics},
                    "contrast_magnitude": _vector_magnitude(deltas),
                    "dominant_metric": dominant_metric,
                }
            )

    return pd.DataFrame.from_records(records)


def plot_selected_segment_treatment_heatmap(
    treatment_contrasts,
    metrics,
    color_limit=1.5,
    title="Treatment drill-down within selected behavioral segments",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="selected_segment_treatment_contrasts.png",
    close=None,
):
    """Plot supported Treatment-level Saved-minus-Stopped contrasts."""
    plot_data = treatment_contrasts[treatment_contrasts["supported"]].copy()
    if plot_data.empty:
        return None

    matrix = plot_data[[f"delta__{metric}" for metric in metrics]].copy()
    matrix.columns = metrics
    matrix.index = [
        (
            f"{rank}. {label} · {treatment} "
            f"(n={saved_n:,}/{stopped_n:,})"
        )
        for rank, label, treatment, saved_n, stopped_n in zip(
            plot_data["segment_rank"],
            plot_data["segment_label"],
            plot_data["Treatment"],
            plot_data["n__saved"],
            plot_data["n__stopped"],
        )
    ]
    limit = float(color_limit)
    fig_height = max(4.0, 0.48 * len(matrix) + 1.8)
    fig_width = max(9.0, 1.8 * len(metrics) + 6.5)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    sns.heatmap(
        matrix,
        ax=ax,
        cmap="vlag",
        center=0,
        vmin=-limit,
        vmax=limit,
        annot=True,
        fmt=".2f",
        linewidths=0.5,
        linecolor="white",
        cbar_kws={"label": "Saved minus Stopped median (IQR units)"},
    )
    ax.set_title(title)
    ax.set_xlabel("Behavior metric")
    ax.set_ylabel("Selected segment × Treatment")
    ax.tick_params(axis="x", rotation=0)
    ax.tick_params(axis="y", rotation=0)
    saved_path = _finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        close=close,
    )
    ax._saved_path = Path(saved_path) if saved_path is not None else None
    return ax
