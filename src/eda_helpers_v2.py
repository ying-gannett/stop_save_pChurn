"""Behavioral profiling helpers for the stop/save EDA v2 workflow.

The v2 workflow replaces exhaustive segment boxplots with compact profile and
outcome-contrast matrices. All scores use one reference fitted on the complete
contacted population so Saved and Stopped users remain directly comparable.
"""

import textwrap
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from src.eda_helpers import save_chart


ID_COL = "billing_account"
OUTCOME_COL = "outcome"
SAVED = "Saved"
STOPPED = "Stopped"
OUTCOMES = (SAVED, STOPPED)
OUTCOME_COLORS = {
    SAVED: "#2E8B57",
    STOPPED: "#C44E52",
}


def fit_behavior_reference(data, metrics):
    """Fit common clipping and robust-scaling values on all contacted users."""
    values = data[metrics]
    lower_bounds = values.quantile(0.01)
    upper_bounds = values.quantile(0.99)
    centers = values.median()
    spreads = values.quantile(0.75) - values.quantile(0.25)

    fallback_spreads = upper_bounds - lower_bounds
    spreads = spreads.mask(spreads.eq(0), fallback_spreads)
    spreads = spreads.mask(spreads.eq(0), 1.0).fillna(1.0)

    return {
        "lower_bounds": lower_bounds,
        "upper_bounds": upper_bounds,
        "centers": centers,
        "spreads": spreads,
    }


def _clip_metric_values(data, metrics, reference):
    """Clip metrics to the common contacted-population bounds."""
    return data[metrics].clip(
        lower=reference["lower_bounds"][metrics],
        upper=reference["upper_bounds"][metrics],
        axis=1,
    )


def transform_behavior_metrics(data, metrics, reference):
    """Clip and scale metrics into contacted-population median/IQR units."""
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
    """Summarize absolute segment profiles against one contacted-user reference."""
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
        record = {
            **segment_values,
            "segment_label": _segment_label(segment_values, segment_fields),
            "users": users,
            **{f"median__{metric}": raw_medians[metric] for metric in metrics},
            **{f"score__{metric}": score_medians[metric] for metric in metrics},
            "profile_magnitude": _vector_magnitude(score_medians),
            "dominant_metric": (
                finite_scores.abs().idxmax() if not finite_scores.empty else None
            ),
        }
        records.append(record)

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
    """Compare Saved with Stopped users in matched segment slices."""
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
        record = {
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
        records.append(record)

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


def _finalize_figure(
    fig,
    show,
    save,
    chart_folder,
    file_name,
    close,
    tight_layout_kwargs=None,
):
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


def plot_behavior_profile_heatmap(
    profiles,
    metrics,
    max_rows=30,
    color_limit=2.0,
    title="Behavior profiles relative to all contacted users",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="behavior_profiles_v2.png",
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
        cbar_kws={"label": "Median relative to contacted population (IQR units)"},
    )
    ax.set_title(title)
    ax.set_xlabel("Behavior metric")
    ax.set_ylabel("Segment")
    ax.tick_params(axis="x", rotation=0)
    ax.tick_params(axis="y", rotation=0)
    saved_path = _finalize_figure(
        fig,
        show,
        save,
        chart_folder,
        file_name,
        close,
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
    file_name="saved_minus_stopped_profiles_v2.png",
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
    saved_path = _finalize_figure(
        fig,
        show,
        save,
        chart_folder,
        file_name,
        close,
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
    """Build one clipped spread and uncertainty table for selected contrasts.

    Counts and medians are carried forward from ``contrasts``. Only clipped
    quartiles, non-null counts, and optional bootstrap intervals are calculated
    from the account-level data.
    """
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
                metric_values = clipped_values.loc[
                    mask,
                    metric,
                ].dropna()
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
    file_name="selected_segments_raw_clipped_v2.png",
    close=None,
):
    """Plot clipped business-unit values for the selected contrast rows."""
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
    saved_path = _finalize_figure(
        fig,
        show,
        save,
        chart_folder,
        file_name,
        close,
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
    file_name="top_behavior_contrasts_v2.png",
    close=None,
):
    """Drill into only the strongest supported outcome contrasts with boxplots."""
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
        ax.set_ylabel("Contacted-population median/IQR units")
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
    saved_path = _finalize_figure(
        fig,
        show,
        save,
        chart_folder,
        file_name,
        close,
    )
    visible_axes = [ax for ax in flat_axes if ax.get_visible()]
    for ax in visible_axes:
        ax._saved_path = Path(saved_path) if saved_path is not None else None
    return visible_axes
