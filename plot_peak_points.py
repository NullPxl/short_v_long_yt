from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")

REQUIRED_COLUMNS = {"captured_at", "video_id", "view_count"}


def extract_peak_points(csv_path: Path, rolling_window: int) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df = df.dropna(subset=["captured_at", "video_id", "view_count"])
    df = df.sort_values(["video_id", "captured_at"])

    df["time_offset_sec"] = (
        df["captured_at"] - df.groupby("video_id")["captured_at"].transform("first")
    ).dt.total_seconds()
    df["d_views"] = df.groupby("video_id")["view_count"].diff()
    df["dt"] = df.groupby("video_id")["time_offset_sec"].diff()
    df["views_per_sec"] = df["d_views"] / df["dt"]
    df.loc[df["dt"] <= 0, "views_per_sec"] = pd.NA

    df["views_per_sec_smooth"] = (
        df.groupby("video_id")["views_per_sec"]
        .transform(lambda x: x.rolling(rolling_window, min_periods=1).mean())
    )

    peaks = []
    for video_id, g in df.groupby("video_id", sort=False):
        g = g[["time_offset_sec", "views_per_sec_smooth"]].dropna()
        if g.empty:
            continue
        idx = g["views_per_sec_smooth"].idxmax()
        peaks.append(
            {
                "video_id": video_id,
                "peak_time_hours": float(g.loc[idx, "time_offset_sec"]) / 3600.0,
                "peak_views_per_sec": float(g.loc[idx, "views_per_sec_smooth"]),
            }
        )
    return pd.DataFrame(peaks)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot per-item peak growth points for regular videos vs shorts."
    )
    parser.add_argument("--video-input", type=Path, default=Path("video_stats.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats.csv"))
    parser.add_argument("--rolling-window", type=int, default=20)
    parser.add_argument("--output", type=Path, default=Path("plots/peak_points_videos_vs_shorts.png"))
    args = parser.parse_args()

    if args.rolling_window <= 0:
        raise ValueError("--rolling-window must be > 0")

    video_peaks = extract_peak_points(args.video_input, args.rolling_window)
    shorts_peaks = extract_peak_points(args.shorts_input, args.rolling_window)

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.scatter(
        video_peaks["peak_time_hours"],
        video_peaks["peak_views_per_sec"],
        alpha=0.8,
        s=40,
        label=f"Videos (n={len(video_peaks)})",
    )
    ax.scatter(
        shorts_peaks["peak_time_hours"],
        shorts_peaks["peak_views_per_sec"],
        alpha=0.8,
        s=40,
        label=f"Shorts (n={len(shorts_peaks)})",
    )
    ax.set_title("Peak Growth Points by Item")
    ax.set_xlabel("Time to Peak (hours since first seen)")
    ax.set_ylabel("Peak Growth Rate (views/sec, smoothed)")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    plt.close(fig)

    print(f"Done. Saved peak-points plot to: {args.output.resolve()}")


if __name__ == "__main__":
    main()
