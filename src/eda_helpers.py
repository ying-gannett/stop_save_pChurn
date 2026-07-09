import re
from pathlib import Path

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


def build_segment_summary(data, segment, id_col="billing_account"):
    """Aggregate user counts and numeric metric summaries by one segment."""
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
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
):
    """Plot one numeric metric as a boxplot across groups."""
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

    owns_figure = ax is None
    if owns_figure:
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

    resolved_title = chart_title or title or f"{metric} by {group_col}"
    ax.set_title(resolved_title)
    ax.set_xlabel(xlabel or group_col)
    ax.set_ylabel(ylabel or metric)

    if owns_figure:
        __finalize_chart(
            fig,
            show=show,
            save=save,
            chart_folder=chart_folder,
            file_name=file_name,
            chart_title=resolved_title,
            close=close,
            save_kwargs=save_kwargs,
        )

    return ax


def plot_metrics_by_group(
    data,
    metrics,
    group_col,
    title_template=None,
    n_cols=2,
    figsize=(8, 5),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
    **kwargs,
):
    """Plot multiple numeric metrics in one grouped boxplot."""
    metrics = list(metrics)
    if not metrics:
        print("No metrics provided.")
        return None

    order = kwargs.pop("order", None)
    title = kwargs.pop("title", None)
    xlabel = kwargs.pop("xlabel", "Metric")
    ylabel = kwargs.pop("ylabel", "Value")
    id_col = kwargs.pop("id_col", "billing_account")
    min_n = kwargs.pop("min_n", 1)
    show_counts = kwargs.pop("show_counts", True)
    show_points = kwargs.pop("show_points", True)
    showfliers = kwargs.pop("showfliers", True)
    rotate_xticks = kwargs.pop("rotate_xticks", False)
    point_kwargs = kwargs.pop("point_kwargs", None)
    display_counts_on_empty = kwargs.pop("display_counts_on_empty", False)
    palette = kwargs.pop("palette", None)
    metric_label = kwargs.pop("metric_label", "metric")
    value_label = kwargs.pop("value_label", "value")
    boxplot_kwargs = kwargs.pop("boxplot_kwargs", {})
    boxplot_kwargs.update(kwargs)

    if group_col not in data.columns:
        print(f"Missing required columns: ['{group_col}']")
        return None

    available_metrics = [metric for metric in metrics if metric in data.columns]
    missing_metrics = [metric for metric in metrics if metric not in data.columns]
    if missing_metrics:
        print(f"Missing metric columns skipped: {missing_metrics}")

    if not available_metrics:
        print("No metric columns available to plot.")
        return None

    plot_source = data.dropna(subset=[group_col]).copy()
    plot_source = plot_source[plot_source[available_metrics].notna().any(axis=1)]

    if id_col in plot_source.columns:
        counts = __get_group_counts(plot_source, group_col, id_col=id_col)
    else:
        counts = plot_source.groupby(group_col, dropna=True).size()

    resolved_order = __resolve_group_order(counts, order=order, min_n=min_n)
    plot_source = plot_source[plot_source[group_col].isin(resolved_order)]

    if plot_source.empty or not resolved_order:
        if display_counts_on_empty:
            print("No groups meet the minimum sample size.")
            display(counts.rename("users").reset_index().sort_values("users", ascending=False))
        return None

    plot_df = plot_source.melt(
        id_vars=[group_col],
        value_vars=available_metrics,
        var_name=metric_label,
        value_name=value_label,
    ).dropna(subset=[value_label])
    plot_df[metric_label] = pd.Categorical(
        plot_df[metric_label],
        categories=available_metrics,
        ordered=True,
    )

    if plot_df.empty:
        print("No non-null metric values available to plot.")
        return None

    resolved_title = chart_title or title
    if resolved_title is None and title_template is not None:
        try:
            resolved_title = title_template.format(metric="Metrics", group_col=group_col)
        except (KeyError, IndexError):
            resolved_title = title_template
    if resolved_title is None:
        resolved_title = f"Metrics by {group_col}"

    fig, ax = plt.subplots(figsize=figsize)
    sns.boxplot(
        data=plot_df,
        x=metric_label,
        y=value_label,
        hue=group_col,
        hue_order=resolved_order,
        palette=palette,
        showfliers=showfliers,
        ax=ax,
        **boxplot_kwargs,
    )

    if show_points:
        stripplot_kwargs = {
            "alpha": 0.25,
            "size": 3,
            "jitter": 0.2,
            "dodge": True,
            "legend": False,
        }
        if point_kwargs is not None:
            stripplot_kwargs.update(point_kwargs)

        sns.stripplot(
            data=plot_df,
            x=metric_label,
            y=value_label,
            hue=group_col,
            hue_order=resolved_order,
            palette=palette,
            ax=ax,
            **stripplot_kwargs,
        )

    if rotate_xticks:
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.set_title(resolved_title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    legend = ax.get_legend()
    if legend is not None:
        legend.set_title(group_col)
        if show_counts:
            label_map = {
                str(group): f"{group} (n={counts.loc[group]:,})"
                for group in resolved_order
            }
            for text in legend.get_texts():
                text.set_text(label_map.get(text.get_text(), text.get_text()))

    __finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        chart_title=resolved_title,
        close=close,
        save_kwargs=save_kwargs,
    )
    return ax


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


def plot_full_and_clipped_boxplot(
    data,
    metric,
    group,
    lower_q=0.01,
    upper_q=0.99,
    figsize=(10, 3),
    show=False,
    save=True,
    chart_folder="charts",
    file_name=None,
    chart_title=None,
    close=None,
    save_kwargs=None,
):
    """Plot full and percentile-clipped boxplots for one numeric metric."""
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

    __finalize_chart(
        fig,
        show=show,
        save=save,
        chart_folder=chart_folder,
        file_name=file_name,
        chart_title=chart_title or f"{metric} boxplot | {group} users",
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
        plot_metrics_by_group(
            data=plot_df,
            metrics=metrics,
            group_col=group_col,
            order=treatment_order,
            min_n=min_n,
            figsize=(9, 5),
            display_counts_on_empty=False,
            show=show,
            save=save,
            chart_folder=chart_folder,
            file_name=file_name,
            chart_title=f"{title_filters}\n\nMetrics by {group_col}",
            close=close,
            save_kwargs=save_kwargs,
        )
    return segment_combo_counts
