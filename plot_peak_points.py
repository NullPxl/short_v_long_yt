from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")

REQUIRED_COLUMNS = {"captured_at", "video_id", "view_count", "like_count", "comment_count"}


def extract_peak_points(csv_path: Path, metric_col: str, rolling_window: int) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")
    df = df.dropna(subset=["captured_at", "video_id", metric_col])
    df = df.sort_values(["video_id", "captured_at"])

    df["time_offset_sec"] = (
        df["captured_at"] - df.groupby("video_id")["captured_at"].transform("first")
    ).dt.total_seconds()
    df["d_metric"] = df.groupby("video_id")[metric_col].diff()
    df["dt"] = df.groupby("video_id")["time_offset_sec"].diff()
    df["metric_per_sec"] = df["d_metric"] / df["dt"]
    df.loc[df["dt"] <= 0, "metric_per_sec"] = pd.NA

    df["metric_per_sec_smooth"] = (
        df.groupby("video_id")["metric_per_sec"]
        .transform(lambda x: x.rolling(rolling_window, min_periods=1).mean())
    )

    peaks = []
    for video_id, g in df.groupby("video_id", sort=False):
        g = g[["time_offset_sec", "metric_per_sec_smooth"]].dropna()
        if g.empty:
            continue
        idx = g["metric_per_sec_smooth"].idxmax()
        peaks.append(
            {
                "video_id": video_id,
                "peak_time_hours": float(g.loc[idx, "time_offset_sec"]) / 3600.0,
                "peak_metric_per_sec": float(g.loc[idx, "metric_per_sec_smooth"]),
            }
        )
    return pd.DataFrame(peaks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot per-item peak growth points for videos vs shorts.")
    parser.add_argument("--video-input", type=Path, default=Path("video_stats.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats.csv"))
    parser.add_argument("--rolling-window", type=int, default=20)
    parser.add_argument("--output-dir", type=Path, default=Path("plots"))
    args = parser.parse_args()

    if args.rolling_window <= 0:
        raise ValueError("--rolling-window must be > 0")

    metric_specs = [
        ("view_count", "Views", "peak_points_views_videos_vs_shorts.png"),
        ("like_count", "Likes", "peak_points_likes_videos_vs_shorts.png"),
        ("comment_count", "Comments", "peak_points_comments_videos_vs_shorts.png"),
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)

    for metric_col, metric_label, filename in metric_specs:
        video_peaks = extract_peak_points(args.video_input, metric_col, args.rolling_window)
        shorts_peaks = extract_peak_points(args.shorts_input, metric_col, args.rolling_window)

        fig, ax = plt.subplots(figsize=(11, 6))
        ax.scatter(
            video_peaks["peak_time_hours"],
            video_peaks["peak_metric_per_sec"],
            alpha=0.8,
            s=40,
            label=f"Videos (n={len(video_peaks)})",
        )
        ax.scatter(
            shorts_peaks["peak_time_hours"],
            shorts_peaks["peak_metric_per_sec"],
            alpha=0.8,
            s=40,
            label=f"Shorts (n={len(shorts_peaks)})",
        )
        ax.set_title(f"Peak {metric_label} Growth Points by Item")
        ax.set_xlabel("Time to Peak (hours since first seen)")
        ax.set_ylabel(f"Peak {metric_label} Growth Rate ({metric_label.lower()}/sec, smoothed)")
        ax.grid(True, alpha=0.25)
        ax.legend(loc="best")

        out_path = args.output_dir / filename
        fig.tight_layout()
        fig.savefig(out_path, dpi=150)
        plt.close(fig)

    print(f"Done. Saved 3 peak-point plots to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
