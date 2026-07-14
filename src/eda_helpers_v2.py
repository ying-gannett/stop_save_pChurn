"""Behavioral profiling helpers for the stop/save EDA v2 workflow.

The v2 workflow replaces exhaustive segment boxplots with compact profile and
outcome-contrast matrices. All scores use one reference fitted on the complete
contacted population so Saved and Stopped users remain directly comparable.
"""

import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from src.eda_helpers import save_chart


DEFAULT_OUTCOME_COLORS = {
    "saved": "#2E8B57",
    "stopped": "#C44E52",
    "stoped": "#C44E52",
}


def _require_columns(data, columns):
    """Raise a clear error when required columns are absent."""
    missing = [column for column in columns if column not in data.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")


def _validate_unique_accounts(data, id_col):
    """Enforce the one-row-per-contacted-account profiling unit."""
    if data.empty:
        raise ValueError("Behavioral profiling data must not be empty.")
    if data[id_col].isna().any():
        raise ValueError(f"{id_col} must not contain missing values.")
    if data[id_col].duplicated().any():
        raise ValueError(
            f"{id_col} must be unique; behavioral profiles use one row per account."
        )


def _validate_quantiles(lower_q, upper_q):
    if not 0 <= lower_q < upper_q <= 1:
        raise ValueError(
            "lower_q and upper_q must satisfy 0 <= lower_q < upper_q <= 1."
        )


def _numeric_metric_values(data, metrics):
    values = data[list(metrics)].apply(pd.to_numeric, errors="coerce")
    empty_metrics = values.columns[values.notna().sum().eq(0)].tolist()
    if empty_metrics:
        raise ValueError(f"Metrics contain no numeric observations: {empty_metrics}")
    return values.astype("float64")


def fit_behavior_reference(
    data,
    metrics,
    lower_q=0.01,
    upper_q=0.99,
):
    """Fit common clipping and robust-scaling values on all contacted users."""
    metrics = list(metrics)
    _require_columns(data, metrics)
    _validate_quantiles(lower_q, upper_q)

    values = _numeric_metric_values(data, metrics)
    lower_bounds = values.quantile(lower_q)
    upper_bounds = values.quantile(upper_q)
    centers = values.median()
    spreads = values.quantile(0.75) - values.quantile(0.25)

    fallback_spreads = upper_bounds - lower_bounds
    spreads = spreads.mask(spreads.eq(0), fallback_spreads)
    spreads = spreads.mask(spreads.eq(0), 1.0).fillna(1.0)

    return {
        "metrics": tuple(metrics),
        "lower_q": lower_q,
        "upper_q": upper_q,
        "lower_bounds": lower_bounds,
        "upper_bounds": upper_bounds,
        "centers": centers,
        "spreads": spreads,
    }


def _validate_behavior_reference(reference, metrics):
    required_keys = {"lower_bounds", "upper_bounds", "centers", "spreads"}
    missing_keys = required_keys.difference(reference)
    if missing_keys:
        raise KeyError(f"Behavior reference is missing keys: {sorted(missing_keys)}")

    for key in required_keys:
        missing_metrics = set(metrics).difference(reference[key].index)
        if missing_metrics:
            raise KeyError(
                f"Behavior reference {key!r} is missing metrics: "
                f"{sorted(missing_metrics)}"
            )


def transform_behavior_metrics(data, metrics, reference):
    """Clip and scale metrics into contacted-population median/IQR units."""
    metrics = list(metrics)
    _require_columns(data, metrics)
    _validate_behavior_reference(reference, metrics)

    values = _numeric_metric_values(data, metrics)
    clipped = values.clip(
        lower=reference["lower_bounds"][metrics],
        upper=reference["upper_bounds"][metrics],
        axis=1,
    )
    return clipped.sub(reference["centers"][metrics], axis=1).div(
        reference["spreads"][metrics],
        axis=1,
    )


def _display_value(value):
    if pd.isna(value):
        return "Missing"
    return str(value)


def _segment_label(values, segment_fields):
    return " · ".join(_display_value(values[field]) for field in segment_fields)


def _iter_group_keys(data, segment_fields):
    grouped = data.groupby(
        segment_fields,
        dropna=False,
        observed=True,
        sort=False,
    )
    for key, group in grouped:
        if len(segment_fields) == 1 and not isinstance(key, tuple):
            key = (key,)
        yield dict(zip(segment_fields, key)), group


def _score_summary(score_values, metrics, prefix):
    medians = score_values[list(metrics)].median()
    return {f"{prefix}__{metric}": medians[metric] for metric in metrics}


def _raw_median_summary(data, metrics, prefix="median"):
    medians = data[list(metrics)].apply(pd.to_numeric, errors="coerce").median()
    return {f"{prefix}__{metric}": medians[metric] for metric in metrics}


def _vector_magnitude(values):
    finite = np.asarray(values, dtype="float64")
    finite = finite[np.isfinite(finite)]
    if not finite.size:
        return np.nan
    return float(np.sqrt(np.square(finite).sum()))


def _dominant_metric(record, metrics, prefix):
    available = {
        metric: record[f"{prefix}__{metric}"]
        for metric in metrics
        if pd.notna(record[f"{prefix}__{metric}"])
    }
    if not available:
        return None
    return max(available, key=lambda metric: abs(available[metric]))


def build_behavior_profiles(
    data,
    metrics,
    segment_fields,
    id_col="billing_account",
    min_n=20,
    reference=None,
    lower_q=0.01,
    upper_q=0.99,
):
    """Summarize absolute segment profiles against one contacted-user reference."""
    metrics = list(metrics)
    segment_fields = list(segment_fields)
    if not segment_fields:
        raise ValueError("segment_fields must contain at least one column.")
    if min_n < 1:
        raise ValueError("min_n must be at least 1.")

    _require_columns(data, [id_col, *metrics, *segment_fields])
    _validate_unique_accounts(data, id_col)
    working = data.reset_index(drop=True).copy()
    if reference is None:
        reference = fit_behavior_reference(
            working,
            metrics,
            lower_q=lower_q,
            upper_q=upper_q,
        )
    scores = transform_behavior_metrics(working, metrics, reference)
    score_columns = {metric: f"__behavior_score__{metric}" for metric in metrics}
    working = working.join(scores.rename(columns=score_columns))

    records = []
    for segment_values, group in _iter_group_keys(working, segment_fields):
        users = group[id_col].nunique()
        if users < min_n:
            continue

        group_scores = group[[score_columns[metric] for metric in metrics]].rename(
            columns={score_columns[metric]: metric for metric in metrics}
        )
        record = {
            **segment_values,
            "segment_label": _segment_label(segment_values, segment_fields),
            "users": users,
            **_raw_median_summary(group, metrics),
            **_score_summary(group_scores, metrics, "score"),
        }
        score_vector = [record[f"score__{metric}"] for metric in metrics]
        record["profile_magnitude"] = _vector_magnitude(score_vector)
        record["dominant_metric"] = _dominant_metric(record, metrics, "score")
        records.append(record)

    result = pd.DataFrame.from_records(records)
    if not result.empty:
        result = result.sort_values(
            ["profile_magnitude", "users"],
            ascending=[False, False],
        ).reset_index(drop=True)
    result.attrs["metric_reference"] = reference
    result.attrs["metrics"] = tuple(metrics)
    result.attrs["segment_fields"] = tuple(segment_fields)
    result.attrs["min_n"] = min_n
    return result


def _column_token(value):
    token = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    return token or "outcome"


def build_outcome_contrasts(
    data,
    metrics,
    segment_fields,
    outcome_col="status",
    outcomes=("saved", "stoped"),
    id_col="billing_account",
    min_n=20,
    reference=None,
    lower_q=0.01,
    upper_q=0.99,
):
    """Compare two outcomes within identical business-defined segment slices.

    Delta scores are the first outcome minus the second outcome in common
    contacted-population IQR units.
    """
    metrics = list(metrics)
    segment_fields = list(segment_fields)
    outcomes = tuple(outcomes)
    if not segment_fields:
        raise ValueError("segment_fields must contain at least one column.")
    if len(outcomes) != 2 or outcomes[0] == outcomes[1]:
        raise ValueError("outcomes must contain two distinct values.")
    if min_n < 1:
        raise ValueError("min_n must be at least 1.")

    _require_columns(data, [id_col, outcome_col, *metrics, *segment_fields])
    filtered = data[data[outcome_col].isin(outcomes)].reset_index(drop=True).copy()
    _validate_unique_accounts(filtered, id_col)
    if reference is None:
        reference = fit_behavior_reference(
            filtered,
            metrics,
            lower_q=lower_q,
            upper_q=upper_q,
        )
    scores = transform_behavior_metrics(filtered, metrics, reference)
    score_columns = {metric: f"__behavior_score__{metric}" for metric in metrics}
    filtered = filtered.join(scores.rename(columns=score_columns))

    outcome_tokens = tuple(_column_token(outcome) for outcome in outcomes)
    if outcome_tokens[0] == outcome_tokens[1]:
        raise ValueError("outcomes must resolve to distinct column labels.")

    records = []
    for segment_values, group in _iter_group_keys(filtered, segment_fields):
        outcome_groups = {
            outcome: group[group[outcome_col].eq(outcome)] for outcome in outcomes
        }
        outcome_counts = {
            outcome: frame[id_col].nunique()
            for outcome, frame in outcome_groups.items()
        }
        if any(count < min_n for count in outcome_counts.values()):
            continue

        record = {
            **segment_values,
            "segment_label": _segment_label(segment_values, segment_fields),
        }
        score_medians = {}
        for outcome, token in zip(outcomes, outcome_tokens):
            frame = outcome_groups[outcome]
            record[f"n__{token}"] = outcome_counts[outcome]
            record.update(_raw_median_summary(frame, metrics, f"median_{token}"))
            frame_scores = frame[
                [score_columns[metric] for metric in metrics]
            ].rename(columns={score_columns[metric]: metric for metric in metrics})
            score_medians[outcome] = frame_scores.median()

        for metric in metrics:
            record[f"delta__{metric}"] = (
                score_medians[outcomes[0]][metric]
                - score_medians[outcomes[1]][metric]
            )

        total_users = sum(outcome_counts.values())
        record["total_users"] = total_users
        record[f"observed_{outcome_tokens[0]}_share"] = (
            outcome_counts[outcomes[0]] / total_users
        )
        delta_vector = [record[f"delta__{metric}"] for metric in metrics]
        record["contrast_magnitude"] = _vector_magnitude(delta_vector)
        record["dominant_metric"] = _dominant_metric(record, metrics, "delta")
        dominant_metric = record["dominant_metric"]
        if dominant_metric is None:
            record["dominant_outcome"] = None
        elif record[f"delta__{dominant_metric}"] >= 0:
            record["dominant_outcome"] = outcomes[0]
        else:
            record["dominant_outcome"] = outcomes[1]
        records.append(record)

    result = pd.DataFrame.from_records(records)
    if not result.empty:
        result = result.sort_values(
            ["contrast_magnitude", "total_users"],
            ascending=[False, False],
        ).reset_index(drop=True)
    result.attrs["metric_reference"] = reference
    result.attrs["metrics"] = tuple(metrics)
    result.attrs["segment_fields"] = tuple(segment_fields)
    result.attrs["outcome_col"] = outcome_col
    result.attrs["outcomes"] = outcomes
    result.attrs["min_n"] = min_n
    return result


def _resolve_heatmap_limit(matrix, color_limit):
    if color_limit is not None:
        if color_limit <= 0:
            raise ValueError("color_limit must be positive.")
        return float(color_limit)

    values = np.abs(matrix.to_numpy(dtype="float64"))
    finite = values[np.isfinite(values)]
    return max(float(finite.max()), 0.5) if finite.size else 0.5


def _finalize_figure(
    fig,
    show,
    save,
    chart_folder,
    file_name,
    close,
    save_kwargs,
):
    fig.tight_layout()
    saved_path = None
    if save:
        saved_path = save_chart(
            fig,
            folder=chart_folder,
            file_name=file_name,
            **(save_kwargs or {}),
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
    annotate=True,
    title="Behavior profiles relative to all contacted users",
    cmap="vlag",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="behavior_profiles_v2.png",
    close=None,
    save_kwargs=None,
):
    """Plot absolute segment profile scores as a compact heatmap."""
    metrics = list(metrics)
    required = ["segment_label", "users", *[f"score__{m}" for m in metrics]]
    _require_columns(profiles, required)
    if profiles.empty:
        raise ValueError("No behavior profiles are available to plot.")
    if max_rows is not None and max_rows < 1:
        raise ValueError("max_rows must be at least 1 or None.")

    plot_data = profiles if max_rows is None else profiles.head(max_rows)
    matrix = plot_data[[f"score__{metric}" for metric in metrics]].copy()
    matrix.columns = metrics
    matrix.index = [
        f"{label}  (n={users:,})"
        for label, users in zip(plot_data["segment_label"], plot_data["users"])
    ]
    limit = _resolve_heatmap_limit(matrix, color_limit)
    fig_height = max(4.0, 0.42 * len(matrix) + 1.8)
    fig_width = max(8.0, 1.8 * len(metrics) + 5.5)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    sns.heatmap(
        matrix,
        ax=ax,
        cmap=cmap,
        center=0,
        vmin=-limit,
        vmax=limit,
        annot=annotate,
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
        save_kwargs,
    )
    ax._saved_path = Path(saved_path) if saved_path is not None else None
    return ax


def plot_outcome_contrast_heatmap(
    contrasts,
    metrics,
    max_rows=20,
    color_limit=1.5,
    annotate=True,
    title=None,
    cmap="vlag",
    show=False,
    save=True,
    chart_folder="charts",
    file_name="saved_minus_stopped_profiles_v2.png",
    close=None,
    save_kwargs=None,
):
    """Plot within-slice outcome differences in common IQR units."""
    metrics = list(metrics)
    required = [
        "segment_label",
        *[f"delta__{metric}" for metric in metrics],
    ]
    _require_columns(contrasts, required)
    if contrasts.empty:
        raise ValueError("No outcome contrasts are available to plot.")
    if max_rows is not None and max_rows < 1:
        raise ValueError("max_rows must be at least 1 or None.")

    outcomes = contrasts.attrs.get("outcomes", ("Outcome A", "Outcome B"))
    tokens = tuple(_column_token(outcome) for outcome in outcomes)
    count_columns = [f"n__{token}" for token in tokens]
    _require_columns(contrasts, count_columns)

    plot_data = contrasts if max_rows is None else contrasts.head(max_rows)
    matrix = plot_data[[f"delta__{metric}" for metric in metrics]].copy()
    matrix.columns = metrics
    matrix.index = [
        f"{label}  (n={n_a:,}/{n_b:,})"
        for label, n_a, n_b in zip(
            plot_data["segment_label"],
            plot_data[count_columns[0]],
            plot_data[count_columns[1]],
        )
    ]
    limit = _resolve_heatmap_limit(matrix, color_limit)
    fig_height = max(4.0, 0.48 * len(matrix) + 1.8)
    fig_width = max(8.0, 1.8 * len(metrics) + 5.5)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    sns.heatmap(
        matrix,
        ax=ax,
        cmap=cmap,
        center=0,
        vmin=-limit,
        vmax=limit,
        annot=annotate,
        fmt=".2f",
        linewidths=0.5,
        linecolor="white",
        cbar_kws={
            "label": f"{outcomes[0]} minus {outcomes[1]} median (IQR units)"
        },
    )
    resolved_title = title or f"Behavior contrast: {outcomes[0]} minus {outcomes[1]}"
    ax.set_title(resolved_title)
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
        save_kwargs,
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


def _resolve_outcome_palette(outcomes, palette):
    if palette is not None:
        return palette
    return {
        outcome: DEFAULT_OUTCOME_COLORS.get(
            str(outcome).strip().lower(),
            sns.color_palette("deep", n_colors=len(outcomes))[index],
        )
        for index, outcome in enumerate(outcomes)
    }


def plot_top_behavior_contrasts(
    data,
    contrasts,
    metrics,
    segment_fields,
    reference,
    outcome_col="status",
    outcomes=("saved", "stoped"),
    top_n=8,
    n_cols=2,
    panel_size=(7, 4),
    palette=None,
    show_points=False,
    point_kwargs=None,
    title=None,
    show=False,
    save=True,
    chart_folder="charts",
    file_name="top_behavior_contrasts_v2.png",
    close=None,
    save_kwargs=None,
):
    """Drill into only the strongest supported outcome contrasts with boxplots."""
    metrics = list(metrics)
    segment_fields = list(segment_fields)
    outcomes = tuple(outcomes)
    if top_n < 1:
        raise ValueError("top_n must be at least 1.")
    if n_cols < 1:
        raise ValueError("n_cols must be at least 1.")
    _require_columns(data, [outcome_col, *metrics, *segment_fields])
    _require_columns(contrasts, ["segment_label", *segment_fields])
    if contrasts.empty:
        raise ValueError("No outcome contrasts are available for drill-down plots.")

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
    resolved_palette = _resolve_outcome_palette(outcomes, palette)
    point_defaults = {"alpha": 0.35, "size": 2.5, "jitter": 0.18}
    point_defaults.update(point_kwargs or {})

    for panel_index, (_, contrast) in enumerate(selected.iterrows()):
        ax = flat_axes[panel_index]
        segment_data = _match_segment(data, contrast, segment_fields)
        segment_data = segment_data[segment_data[outcome_col].isin(outcomes)].copy()
        scores = transform_behavior_metrics(segment_data, metrics, reference)
        long_data = scores.assign(
            **{outcome_col: segment_data[outcome_col].to_numpy()}
        ).melt(
            id_vars=[outcome_col],
            value_vars=metrics,
            var_name="metric",
            value_name="score",
        )
        sns.boxplot(
            data=long_data,
            x="metric",
            y="score",
            hue=outcome_col,
            hue_order=outcomes,
            palette=resolved_palette,
            showfliers=True,
            ax=ax,
        )
        if show_points:
            sns.stripplot(
                data=long_data,
                x="metric",
                y="score",
                hue=outcome_col,
                hue_order=outcomes,
                palette=resolved_palette,
                dodge=True,
                legend=False,
                ax=ax,
                **point_defaults,
            )

        counts = segment_data.groupby(outcome_col, observed=True).size()
        count_text = " | ".join(
            f"{outcome} n={int(counts.get(outcome, 0)):,}" for outcome in outcomes
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
            ax.legend(title=outcome_col)

    for ax in flat_axes[len(selected):]:
        ax.set_visible(False)

    resolved_title = title or f"Top supported behavior contrasts: {outcomes[0]} vs {outcomes[1]}"
    fig.suptitle(resolved_title, fontsize=15, y=1.01)
    saved_path = _finalize_figure(
        fig,
        show,
        save,
        chart_folder,
        file_name,
        close,
        save_kwargs,
    )
    visible_axes = [ax for ax in flat_axes if ax.get_visible()]
    for ax in visible_axes:
        ax._saved_path = Path(saved_path) if saved_path is not None else None
    return visible_axes
