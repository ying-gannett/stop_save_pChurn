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
_GROUP_COLORS = {
    "analysis_population": {
        "no action yet": "#8C8C8C",
        "called once — contacted": "#4C78A8",
    },
    "outcome": {
        "saved": OUTCOME_COLORS[SAVED],
        "stopped": OUTCOME_COLORS[STOPPED],
    },
    "likely_discount_shopper": {
        "other": "#4C78A8",
        "pay less than start rate": "#D5181E",
        "contacted both ways": "#F28E2B"
    },
    "treatment": {
        "control": "#2E8B57",
        "midpoint": "#4C78A8",
        "tiered": "#8E5EA2",
    },
}


def build_distribution_summary(data, numeric_fields):
    """Summarize quality, distribution, and IQR outliers for each behavior metric."""
    records = []
    for field in numeric_fields:
        all_values = data[field]
        values = all_values.dropna()
        non_null_count = len(values)
        percentiles = values.quantile(_PERCENTILES)
        q1 = percentiles.loc[0.25]
        q3 = percentiles.loc[0.75]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outlier_count = ((values < lower_bound) | (values > upper_bound)).sum()
        records.append(
            {
                "field": field,
                "row_count": len(data),
                "non_null_count": non_null_count,
                "null_count": all_values.isna().sum(),
                "null_pct": all_values.isna().mean() * 100,
                "zero_count": all_values.eq(0).sum(),
                "negative_count": all_values.lt(0).sum(),
                "min": values.min(),
                "max": values.max(),
                "mean": values.mean(),
                "median": percentiles.loc[0.50],
                "std": values.std(),
                **{
                    f"p{int(percentile * 100):02d}": value
                    for percentile, value in percentiles.items()
                },
                "iqr": iqr,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "outlier_count": outlier_count,
                "outlier_pct": (
                    outlier_count / non_null_count * 100
                    if non_null_count
                    else np.nan
                ),
            }
        )

    return pd.DataFrame.from_records(records)


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
    """Fit 1%-99% clipping bounds and robust scaling parameters per metric on called-once contacted population."""
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


def clip_metric_values(data, metrics, reference):
    """Clip metrics to the primary contacted-population bounds."""
    return data[metrics].clip(
        lower=reference["lower_bounds"][metrics],
        upper=reference["upper_bounds"][metrics],
        axis=1,
    )


def transform_behavior_metrics(data, metrics, reference):
    """Express clipped metrics in primary-population median/IQR units."""
    clipped = clip_metric_values(data, metrics, reference)
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


def _match_segment(data, segment_values, segment_fields):
    mask = pd.Series(True, index=data.index)
    for field in segment_fields:
        value = segment_values[field]
        if pd.isna(value):
            mask &= data[field].isna()
        else:
            mask &= data[field].eq(value)
    return data[mask]


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
    profiles,
    metrics,
    segment_fields,
):
    """Pair the previously calculated Saved and Stopped profile rows."""
    profile_columns = [
        *segment_fields,
        "users",
        *[f"median__{metric}" for metric in metrics],
        *[f"score__{metric}" for metric in metrics],
    ]
    saved_profiles = profiles.loc[
        profiles[OUTCOME_COL].eq(SAVED),
        profile_columns,
    ].rename(
        columns={
            "users": "n__saved",
            **{
                f"median__{metric}": f"median_saved__{metric}"
                for metric in metrics
            },
            **{
                f"score__{metric}": f"score_saved__{metric}"
                for metric in metrics
            },
        }
    )
    stopped_profiles = profiles.loc[
        profiles[OUTCOME_COL].eq(STOPPED),
        profile_columns,
    ].rename(
        columns={
            "users": "n__stopped",
            **{
                f"median__{metric}": f"median_stopped__{metric}"
                for metric in metrics
            },
            **{
                f"score__{metric}": f"score_stopped__{metric}"
                for metric in metrics
            },
        }
    )
    matched_profiles = saved_profiles.merge(
        stopped_profiles,
        on=segment_fields,
        how="inner",
    )

    records = []
    for _, matched in matched_profiles.iterrows():
        segment_values = {field: matched[field] for field in segment_fields}
        deltas = pd.Series(
            {
                metric: (
                    matched[f"score_saved__{metric}"]
                    - matched[f"score_stopped__{metric}"]
                )
                for metric in metrics
            }
        )
        finite_deltas = deltas.dropna()
        dominant_metric = (
            finite_deltas.abs().idxmax() if not finite_deltas.empty else None
        )
        dominant_outcome = None
        if dominant_metric is not None:
            dominant_outcome = SAVED if deltas[dominant_metric] >= 0 else STOPPED
        total_users = matched["n__saved"] + matched["n__stopped"]
        records.append(
            {
                **segment_values,
                "segment_label": _segment_label(segment_values, segment_fields),
                "n__saved": matched["n__saved"],
                "n__stopped": matched["n__stopped"],
                **{
                    f"median_saved__{metric}": matched[
                        f"median_saved__{metric}"
                    ]
                    for metric in metrics
                },
                **{
                    f"median_stopped__{metric}": matched[
                        f"median_stopped__{metric}"
                    ]
                    for metric in metrics
                },
                **{f"delta__{metric}": deltas[metric] for metric in metrics},
                "total_users": total_users,
                "observed_saved_share": matched["n__saved"] / total_users,
                "contrast_magnitude": _vector_magnitude(deltas),
                "dominant_metric": dominant_metric,
                "dominant_outcome": dominant_outcome,
            }
        )

    if not records:
        raise ValueError("No Saved and Stopped profile rows share a segment.")
    rst = (
        pd.DataFrame.from_records(records)
        .sort_values(
            ["contrast_magnitude", "total_users"],
            ascending=[False, False],
        )
        .reset_index(drop=True)
    )
    rst.insert(0, 'segment_rank', rst.index)
    return rst


def build_treatment_contrasts(
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
    scores = transform_behavior_metrics(data, metrics, reference)
    records = []

    for _, selected_segment in selected.iterrows():
        segment_data = _match_segment(data, selected_segment, segment_fields)
        for treatment in _TREATMENT_ORDER:
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
                    "segment_label": selected_segment["segment_label"]+f" · {treatment}",
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
    """Build clipped business-unit spread and uncertainty details for selected contrasts."""
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

        bootstrap_differences = np.median(
            rng.choice(
                first_values,
                size=(iterations, first_values.size),
                replace=True,
            ),
            axis=1,
        )
        bootstrap_differences -= np.median(
            rng.choice(
                second_values,
                size=(iterations, second_values.size),
                replace=True,
            ),
            axis=1,
        )
        tail_probability = (1 - confidence_level) / 2
        lower, upper = np.quantile(
            bootstrap_differences,
            [tail_probability, 1 - tail_probability],
        )
        return float(lower), float(upper)

    selected = contrasts.head(top_n).reset_index(drop=True).copy()
    rng = np.random.default_rng(random_state)
    records = []

    for _, selected_segment in selected.iterrows():
        segment_data = _match_segment(data, selected_segment, segment_fields)
        clipped_values = clip_metric_values(segment_data, metrics, reference)
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

    if save:
        output_folder = Path(chart_folder)
        output_folder.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_folder / file_name, dpi=300, bbox_inches="tight")

    if show:
        plt.show()

    should_close = not show if close is None else close
    if should_close:
        plt.close(fig)


def plot_metric_boxplot_views(
    data,
    metrics,
    group_col,
    group_order,
    figsize=(15, 4),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    show_points=True,
    rotate_xticks=True,
):
    """Plot full, clipped, and standardized-clipped metric boxplots."""

    def _prepare_metric_boxplot_data(
        data,
        metrics,
        group_col,
        group_order,
    ):
        """Prepare full, clipped, and standardized-clipped boxplot data."""
        plot_source = data[data[group_col].isin(group_order)]
        group_counts = plot_source.groupby(group_col)[ID_COL].nunique()

        full_values = plot_source[metrics].astype("float64")
        metric_reference = fit_behavior_reference(full_values, metrics)
        clipped_values = clip_metric_values(
            full_values,
            metrics,
            metric_reference,
        )
        standardized_clipped_values = clipped_values.sub(
            metric_reference["centers"][metrics],
            axis=1,
        ).div(
            metric_reference["spreads"][metrics],
            axis=1,
        )

        group_values = {group_col: plot_source[group_col].to_numpy()}
        value_frames = [
            full_values.assign(**group_values),
            clipped_values.assign(**group_values),
            standardized_clipped_values.assign(**group_values),
        ]
        plot_frames = []
        for values in value_frames:
            plot_df = values.melt(
                id_vars=group_col,
                value_vars=metrics,
                var_name="metric",
                value_name="value",
            ).dropna(subset=["value"])
            plot_df["metric"] = pd.Categorical(
                plot_df["metric"],
                categories=metrics,
                ordered=True,
            )
            plot_frames.append(plot_df)

        return *plot_frames, group_counts

    def _plot_metric_boxplot_axis(
        ax,
        plot_df,
        title,
        group_col,
        group_order,
        palette,
        show_points=False,
        rotate_xticks=True,
        value_label="Value",
    ):
        """Plot one grouped metric boxplot axis."""
        sns.boxplot(
            data=plot_df,
            x="metric",
            y="value",
            hue=group_col,
            hue_order=group_order,
            palette=palette,
            saturation=1,
            showfliers=True,
            ax=ax,
        )
        ax.set_title(title)
        ax.set_xlabel("Metric")
        ax.set_ylabel(value_label)

        if show_points:
            sns.stripplot(
                data=plot_df,
                x="metric",
                y="value",
                hue=group_col,
                hue_order=group_order,
                palette=palette,
                alpha=0.25,
                size=3,
                jitter=0.2,
                dodge=True,
                legend=False,
                ax=ax,
            )

        if rotate_xticks:
            plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

        return ax

    def _format_metric_boxplot_legend(
        legend,
        group_col,
        group_counts,
        group_order,
    ):
        """Add unique account counts to the grouped metric boxplot legend."""
        legend.set_title(group_col)
        label_map = {
            str(group_value): f"{group_value} (n={group_counts.loc[group_value]:,})"
            for group_value in group_order
        }
        for text in legend.get_texts():
            text.set_text(label_map.get(text.get_text(), text.get_text()))

    prepared = _prepare_metric_boxplot_data(
        data=data,
        metrics=metrics,
        group_col=group_col,
        group_order=group_order,
    )
    (
        full_plot,
        clipped_plot,
        standardized_clipped_plot,
        group_counts,
    ) = prepared
    title = chart_title or f"Metric boxplots by {group_col}"
    semantic_colors = _GROUP_COLORS[group_col.lower()]
    palette = {
        group: semantic_colors[group.lower()]
        for group in group_order
    }

    fig, axes = plt.subplots(1, 3, figsize=figsize)
    common_plot_kwargs = {
        "group_col": group_col,
        "group_order": group_order,
        "palette": palette,
        "show_points": show_points,
        "rotate_xticks": rotate_xticks,
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

    for ax in axes[:-1]:
        ax.get_legend().remove()
    _format_metric_boxplot_legend(
        axes[-1].get_legend(),
        group_col,
        group_counts,
        group_order,
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


def plot_profile_or_contrast_heatmap(
    input_data,
    metrics,
    score_type="profile",
    max_rows=30,
    color_limit=2.0,
    title="Behavior profiles relative to primary contacted users",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="behavior_profiles.png",
    close=None,
):
    """Plot absolute segment profile scores or within-slice outcome differences in common IQR units."""

    def _build_heatmap_artifacts(data, metrics, max_rows=None, score_type="profile"):
        """Prepare the matrix and row labels for a heatmap."""
        plot_data = data.head(max_rows).copy()
        if plot_data.empty:
            return None, None, None, None
        
        if score_type == "profile":
            matrix = plot_data[[f"score__{metric}" for metric in metrics]].copy()
            plot_data.loc[:, 'row_label']=plot_data["segment_label"]+' (n='+plot_data["users"].astype(str)+')'
            y_label="Segment"
            colorbar_label="Median relative to primary population (IQR units)"
        else:
            matrix = plot_data[[f"delta__{metric}" for metric in metrics]].copy()
            colorbar_label="Saved minus Stopped median (IQR units)"
            plot_data.loc[:, 'row_label']=plot_data["segment_rank"].astype(str)+'. '+plot_data["segment_label"]+' (n='+plot_data["n__saved"].astype(str)+'/'+plot_data["n__stopped"].astype(str)+')'
            if 'Treatment' in plot_data.columns:
                y_label="Selected segment × Treatment"
            else:
                y_label="Matched segment"
        
        matrix.columns = metrics
        row_labels = plot_data['row_label'].tolist()
        return matrix, row_labels, y_label, colorbar_label

    def _plot_behavior_heatmap(
        matrix,
        row_labels,
        color_limit,
        title,
        y_label,
        colorbar_label,
        show,
        save,
        chart_folder,
        file_name,
        close,
    ):
        """Render the common heatmap style used by the behavioral analysis."""
        matrix = matrix.copy()
        matrix.index = row_labels
        limit = float(color_limit)
        fig_height = max(4.0, 0.48 * len(matrix) + 1.8)
        fig_width = max(9.0, 1.8 * len(matrix.columns) + 6.0)
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
            cbar_kws={"label": colorbar_label},
        )
        ax.set_title(title)
        ax.set_xlabel("Behavior metric")
        ax.set_ylabel(y_label)
        ax.tick_params(axis="x", rotation=0)
        ax.tick_params(axis="y", rotation=0)
        _finalize_chart(
            fig,
            show=show,
            save=save,
            chart_folder=chart_folder,
            file_name=file_name,
            close=close,
        )
        return ax

    matrix, row_labels, y_label, colorbar_label = _build_heatmap_artifacts(input_data, metrics, max_rows=max_rows, score_type=score_type)
    if matrix is None or row_labels is None:
        return None
    return _plot_behavior_heatmap(
        matrix,
        row_labels,
        color_limit,
        title,
        y_label,
        colorbar_label,
        show,
        save,
        chart_folder,
        file_name,
        close,
    )


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
    """Drill into the strongest supported outcome contrasts with boxplots.

    Treatment-level contrasts retain one panel per selected pooled segment and
    arrange the metric boxes in Treatment-colored bands within that panel.
    """
    treatment_mode = "Treatment" in segment_fields
    base_segment_fields = [
        field for field in segment_fields if field != "Treatment"
    ]
    if treatment_mode:
        selected_ranks = (
            contrasts["segment_rank"].drop_duplicates().head(top_n)
        )
        panels = [
            contrasts[contrasts["segment_rank"].eq(segment_rank)]
            for segment_rank in selected_ranks
        ]
    else:
        panels = [
            contrasts.iloc[[index]]
            for index in range(min(top_n, len(contrasts)))
        ]

    n_rows = int(np.ceil(len(panels) / n_cols))
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(panel_size[0] * n_cols, panel_size[1] * n_rows),
        sharey=True,
        squeeze=False,
    )
    flat_axes = axes.ravel()

    for panel_index, panel in enumerate(panels):
        ax = flat_axes[panel_index]
        contrast = panel.iloc[0]
        match_fields = base_segment_fields if treatment_mode else segment_fields
        segment_data = _match_segment(data, contrast, match_fields)

        treatments = []
        if treatment_mode:
            treatments = [
                treatment
                for treatment in _TREATMENT_ORDER
                if treatment in panel["Treatment"].values
            ]
            segment_data = segment_data[
                segment_data["Treatment"].isin(treatments)
            ]

        scores = transform_behavior_metrics(segment_data, metrics, reference)
        id_columns = {OUTCOME_COL: segment_data[OUTCOME_COL].to_numpy()}
        if treatment_mode:
            id_columns["Treatment"] = segment_data["Treatment"].to_numpy()
        long_data = scores.assign(**id_columns).melt(
            id_vars=list(id_columns),
            value_vars=metrics,
            var_name="metric",
            value_name="score",
        )

        x_field = "metric"
        metric_order = list(metrics)
        if treatment_mode:
            x_field = "treatment_metric"
            long_data[x_field] = (
                long_data["Treatment"] + "__" + long_data["metric"]
            )
            metric_order = [
                f"{treatment}__{metric}"
                for treatment in treatments
                for metric in metrics
            ]

        sns.boxplot(
            data=long_data,
            x=x_field,
            order=metric_order,
            y="score",
            hue=OUTCOME_COL,
            hue_order=OUTCOMES,
            palette=OUTCOME_COLORS,
            showfliers=True,
            ax=ax,
        )

        if treatment_mode:
            counts = segment_data.groupby(
                ["Treatment", OUTCOME_COL], observed=True
            )[ID_COL].nunique()
            for treatment_index, treatment in enumerate(treatments):
                start = treatment_index * len(metrics) - 0.5
                end = (treatment_index + 1) * len(metrics) - 0.5
                background = ax.axvspan(
                    start,
                    end,
                    color=_GROUP_COLORS["treatment"][treatment.lower()],
                    alpha=0.10,
                    zorder=0,
                )
                background.set_gid(f"treatment-background-{treatment}")
                treatment_counts = counts.get(treatment, pd.Series(dtype=int))
                ax.text(
                    (start + end) / 2,
                    1.01,
                    (
                        f"{treatment}\n"
                        f"Saved n={int(treatment_counts.get(SAVED, 0)):,} | "
                        f"Stopped n={int(treatment_counts.get(STOPPED, 0)):,}"
                    ),
                    color=_GROUP_COLORS["treatment"][treatment.lower()],
                    fontsize=8,
                    fontweight="bold",
                    ha="center",
                    va="bottom",
                    transform=ax.get_xaxis_transform(),
                )
                if treatment_index:
                    ax.axvline(start, color="white", linewidth=2, zorder=1)

            ax.set_xticks(range(len(metric_order)))
            ax.set_xticklabels(list(metrics) * len(treatments))
            ax.set_title(
                (
                    f"{contrast['segment_rank']}. "
                    f"{_segment_label(contrast, base_segment_fields)}"
                ),
                fontsize=10,
                pad=38,
            )
            ax.set_xlabel("Behavior metric within Treatment")
        else:
            counts = segment_data.groupby(OUTCOME_COL, observed=True)[
                ID_COL
            ].nunique()
            count_text = " | ".join(
                f"{outcome} n={int(counts.get(outcome, 0)):,}"
                for outcome in OUTCOMES
            )
            ax.set_title(
                (
                    f"{contrast['segment_rank']}. "
                    f"{contrast['segment_label']}\n{count_text}"
                ),
                fontsize=10,
            )
            ax.set_xlabel("Behavior metric")

        ax.axhline(0, color="#666666", linewidth=0.8, linestyle="--")
        ax.set_ylabel("Primary-population median/IQR units")
        ax.tick_params(axis="x", rotation=30)
        if panel_index:
            legend = ax.get_legend()
            if legend is not None:
                legend.remove()
        else:
            ax.legend(title=OUTCOME_COL)

    for ax in flat_axes[len(panels):]:
        ax.set_visible(False)

    fig.suptitle(title, fontsize=15, y=1.01)
    _finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        close=close,
    )
    return [ax for ax in flat_axes if ax.get_visible()]


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
        metric_values = clip_metric_values(segment_data, metrics, reference)

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
                    str(selected_segment["segment_rank"])
                    + ". "
                    + str(selected_segment["segment_label"]),
                    width=42,
                )
                ax.text(
                    -0.5,
                    0.5,
                    wrapped_label,
                    transform=ax.transAxes,
                    ha="right",
                    va="center",
                    fontsize=8.5,
                )
            else:
                ax.set_ylabel("")

    fig.suptitle(title, fontsize=15, x=0.6, y=0.98, ha='center')
    _finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        close=close,
        tight_layout_kwargs={"rect": (0.22, 0, 1, 0.98), "h_pad": 1.0},
    )
    return list(axes.ravel())
