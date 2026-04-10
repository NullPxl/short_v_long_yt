from __future__ import annotations

import argparse
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


REQUIRED_COLUMNS = {
    "captured_at",
    "video_id",
    "view_count",
}

METRICS = ("view_count", "like_count", "comment_count")
EXTRA_PLOT_STEP = "5min"
OPTIONAL_METRICS = ("like_count", "comment_count")
MIN_COMPRESSED_GAP = pd.Timedelta("30min")
COMPRESSED_GAP_WIDTH_STEPS = 2


def format_step_label(step: str) -> str:
    normalized = step.strip().lower()
    labels = {
        "30s": "30-Second",
        "5min": "5-Minute",
        "5m": "5-Minute",
    }
    return labels.get(normalized, step)


def append_suffix(name: str, suffix: str) -> str:
    cleaned = suffix.strip()
    if not cleaned:
        return name
    return f"{name}_{cleaned}"


def load_and_prepare(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["captured_at", "video_id"])

    for metric in OPTIONAL_METRICS:
        if metric not in df.columns:
            df[metric] = 0

    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df = df.dropna(subset=["view_count"])
    for metric in OPTIONAL_METRICS:
        df[metric] = pd.to_numeric(df[metric], errors="coerce").fillna(0)

    df = df.sort_values(["video_id", "captured_at"]).reset_index(drop=True)

    for metric in METRICS:
        new_metric = f"new_{metric}"
        df[new_metric] = (
            df.groupby("video_id", sort=False)[metric]
            .diff()
            .fillna(0)
            .clip(lower=0)
        )

    return df


def aggregate_to_timestep(df: pd.DataFrame, step: str) -> pd.DataFrame:
    df = df.copy()
    df["timestep"] = df["captured_at"].dt.floor(step)

    aggregated = (
        df.groupby("timestep", sort=True)
        .agg(
            rows_captured=("video_id", "size"),
            unique_videos_captured=("video_id", "nunique"),
            total_view_count=("view_count", "sum"),
            total_like_count=("like_count", "sum"),
            total_comment_count=("comment_count", "sum"),
            new_view_count=("new_view_count", "sum"),
            new_like_count=("new_like_count", "sum"),
            new_comment_count=("new_comment_count", "sum"),
        )
        .sort_index()
    )

    if aggregated.empty:
        return aggregated.reset_index()

    full_index = pd.date_range(
        start=aggregated.index.min(),
        end=aggregated.index.max(),
        freq=step,
        tz="UTC",
    )
    aggregated = aggregated.reindex(full_index, fill_value=0)
    aggregated.index.name = "timestep"
    aggregated = aggregated.reset_index()
    aggregated["seconds_since_start"] = (
        aggregated["timestep"] - aggregated["timestep"].min()
    ).dt.total_seconds()

    return aggregated


def write_aggregate_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)


def choose_tick_indices(length: int, max_ticks: int = 8) -> list[int]:
    if length <= max_ticks:
        return list(range(length))

    step = (length - 1) / (max_ticks - 1)
    return sorted({round(i * step) for i in range(max_ticks)})


def build_compressed_positions(
    video_df: pd.DataFrame,
    shorts_df: pd.DataFrame,
    step: str,
) -> tuple[pd.DataFrame, list[tuple[float, float]]]:
    timeline = pd.DataFrame(
        {
            "timestep": sorted(
                set(video_df["timestep"].tolist()) | set(shorts_df["timestep"].tolist())
            )
        }
    )
    if timeline.empty:
        return timeline, []

    timeline = timeline.merge(
        video_df[["timestep", "rows_captured"]].rename(columns={"rows_captured": "video_rows"}),
        on="timestep",
        how="left",
    )
    timeline = timeline.merge(
        shorts_df[["timestep", "rows_captured"]].rename(columns={"rows_captured": "shorts_rows"}),
        on="timestep",
        how="left",
    )
    timeline[["video_rows", "shorts_rows"]] = timeline[["video_rows", "shorts_rows"]].fillna(0)
    timeline["both_missing"] = (timeline["video_rows"] == 0) & (timeline["shorts_rows"] == 0)

    step_delta = pd.Timedelta(step)
    step_seconds = step_delta.total_seconds()
    threshold_bins = max(2, math.ceil(MIN_COMPRESSED_GAP / step_delta))
    compressed_gap_width = step_seconds * COMPRESSED_GAP_WIDTH_STEPS

    positions = [0.0] * len(timeline)
    compressed_spans: list[tuple[float, float]] = []
    i = 1
    while i < len(timeline):
        if bool(timeline.iloc[i]["both_missing"]):
            run_start = i
            while i < len(timeline) and bool(timeline.iloc[i]["both_missing"]):
                i += 1
            run_length = i - run_start

            if run_length >= threshold_bins:
                per_bin = compressed_gap_width / run_length
                gap_start = positions[run_start - 1]
                for j in range(run_start, i):
                    positions[j] = positions[j - 1] + per_bin
                compressed_spans.append((gap_start, positions[i - 1]))
            else:
                for j in range(run_start, i):
                    positions[j] = positions[j - 1] + step_seconds
            continue

        positions[i] = positions[i - 1] + step_seconds
        i += 1

    timeline["plot_x"] = positions
    return timeline[["timestep", "plot_x"]], compressed_spans


def plot_new_views(
    video_df: pd.DataFrame,
    shorts_df: pd.DataFrame,
    step: str,
    step_label: str,
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    positions_df, compressed_spans = build_compressed_positions(video_df, shorts_df, step)
    video_plot_df = video_df.merge(positions_df, on="timestep", how="left")
    shorts_plot_df = shorts_df.merge(positions_df, on="timestep", how="left")

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(
        video_plot_df["plot_x"],
        video_plot_df["new_view_count"],
        label="Videos",
        linewidth=1.8,
    )
    ax.plot(
        shorts_plot_df["plot_x"],
        shorts_plot_df["new_view_count"],
        label="Shorts",
        linewidth=1.8,
    )

    title = f"Aggregate New Views per {step_label} Timestep"
    xlabel = "Captured Timestep (UTC)"
    if compressed_spans:
        title += " (Long Gaps Compressed)"
        xlabel = "Captured Timestep (UTC, long all-missing gaps compressed)"

    for span_start, span_end in compressed_spans:
        ax.axvspan(span_start, span_end, color="0.85", alpha=0.25)

    tick_indices = choose_tick_indices(len(positions_df))
    tick_positions = [positions_df.iloc[index]["plot_x"] for index in tick_indices]
    tick_labels = [
        positions_df.iloc[index]["timestep"].strftime("%Y-%m-%d %H:%M")
        for index in tick_indices
    ]
    ax.set_xticks(tick_positions, tick_labels, rotation=30, ha="right")

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("New Views")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate shorts and regular video stats into 30-second timesteps, "
            "then generate 30-second and 5-minute new-view plots."
        )
    )
    parser.add_argument(
        "--video-input",
        type=Path,
        default=Path("video_stats.csv"),
        help="Regular videos CSV path (default: video_stats.csv)",
    )
    parser.add_argument(
        "--shorts-input",
        type=Path,
        default=Path("shorts_stats.csv"),
        help="Shorts CSV path (default: shorts_stats.csv)",
    )
    parser.add_argument(
        "--step",
        default="30s",
        help="Timestep size for aggregation (default: 30s)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("aggregates"),
        help="Directory to save aggregated CSVs (default: aggregates)",
    )
    parser.add_argument(
        "--plot-path",
        type=Path,
        default=Path("plots") / "aggregate_new_views_video_vs_shorts.png",
        help="Path to save the combined new-views plot",
    )
    parser.add_argument(
        "--name-suffix",
        default="",
        help="Optional suffix to append to generated CSV and plot filenames",
    )
    args = parser.parse_args()

    video_raw = load_and_prepare(args.video_input)
    shorts_raw = load_and_prepare(args.shorts_input)

    video_agg = aggregate_to_timestep(video_raw, args.step)
    shorts_agg = aggregate_to_timestep(shorts_raw, args.step)
    video_agg_5min = aggregate_to_timestep(video_raw, EXTRA_PLOT_STEP)
    shorts_agg_5min = aggregate_to_timestep(shorts_raw, EXTRA_PLOT_STEP)

    video_csv_name = append_suffix("video_stats_30s_timeseries", args.name_suffix) + ".csv"
    shorts_csv_name = append_suffix("shorts_stats_30s_timeseries", args.name_suffix) + ".csv"
    default_plot_base = append_suffix("aggregate_new_views_video_vs_shorts", args.name_suffix)
    default_plot_path = Path("plots") / f"{default_plot_base}.png"
    extra_plot_path = Path("plots") / f"{default_plot_base}_5min.png"

    plot_path = args.plot_path
    if args.name_suffix and args.plot_path == Path("plots") / "aggregate_new_views_video_vs_shorts.png":
        plot_path = default_plot_path

    write_aggregate_csv(video_agg, args.output_dir / video_csv_name)
    write_aggregate_csv(shorts_agg, args.output_dir / shorts_csv_name)
    plot_new_views(video_agg, shorts_agg, args.step, format_step_label(args.step), plot_path)
    plot_new_views(
        video_agg_5min,
        shorts_agg_5min,
        EXTRA_PLOT_STEP,
        format_step_label(EXTRA_PLOT_STEP),
        extra_plot_path,
    )

    print(f"Wrote: {(args.output_dir / video_csv_name).resolve()}")
    print(f"Wrote: {(args.output_dir / shorts_csv_name).resolve()}")
    print(f"Wrote: {plot_path.resolve()}")
    print(f"Wrote: {extra_plot_path.resolve()}")


if __name__ == "__main__":
    main()
