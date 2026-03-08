from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REQUIRED_COLUMNS = {
    "captured_at",
    "video_id",
    "view_count",
    "like_count",
    "comment_count",
}


def load_and_prepare(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["captured_at", "video_id"])

    for col in ("view_count", "like_count", "comment_count"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["view_count", "like_count", "comment_count"])

    df = df.sort_values(["video_id", "captured_at"])
    first_seen = df.groupby("video_id")["captured_at"].transform("min")
    df["hours_since_first_seen"] = (df["captured_at"] - first_seen).dt.total_seconds() / 3600.0

    return df


def _plot_panel(ax: plt.Axes, df: pd.DataFrame, metric: str, ylabel: str, panel_title: str) -> None:
    for video_id, group in df.groupby("video_id", sort=False):
        ax.plot(
            group["hours_since_first_seen"],
            group[metric],
            linewidth=1.2,
            alpha=0.85,
            label=video_id,
        )

    ax.set_title(panel_title)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.25)

    # Hide the legend if there are many videos to keep the chart readable.
    n_videos = df["video_id"].nunique()
    if n_videos <= 20:
        ax.legend(title="video_id", fontsize=7, title_fontsize=8, loc="best")


def plot_metric_pair(
    video_df: pd.DataFrame,
    shorts_df: pd.DataFrame,
    metric: str,
    ylabel: str,
    out_path: Path,
) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(11, 9), sharex=True)
    ax_video, ax_shorts = axes

    _plot_panel(
        ax_video,
        video_df,
        metric,
        ylabel,
        f"Regular Videos: {ylabel} vs Hours Since First Seen",
    )
    _plot_panel(
        ax_shorts,
        shorts_df,
        metric,
        ylabel,
        f"Shorts: {ylabel} vs Hours Since First Seen",
    )
    ax_shorts.set_xlabel("Hours Since First Seen")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot raw per-video growth curves for regular videos vs shorts, aligned to first capture."
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
        "--output-dir",
        type=Path,
        default=Path("plots"),
        help="Directory to save generated plots (default: plots)",
    )
    args = parser.parse_args()

    video_df = load_and_prepare(args.video_input)
    shorts_df = load_and_prepare(args.shorts_input)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    plot_metric_pair(
        video_df,
        shorts_df,
        "view_count",
        "Views",
        args.output_dir / "views_growth_video_vs_shorts.png",
    )
    plot_metric_pair(
        video_df,
        shorts_df,
        "like_count",
        "Likes",
        args.output_dir / "likes_growth_video_vs_shorts.png",
    )
    plot_metric_pair(
        video_df,
        shorts_df,
        "comment_count",
        "Comments",
        args.output_dir / "comments_growth_video_vs_shorts.png",
    )
    print(f"Done. Saved 3 comparison plots to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
