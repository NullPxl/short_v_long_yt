from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


REQUIRED_COLUMNS = {"captured_at", "video_id", "view_count"}


def prepare_growth_rates(csv_path: Path, rolling_window: int, offset_bin_sec: int) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df = df.dropna(subset=["captured_at", "video_id", "view_count"])

    df = df.sort_values(["video_id", "captured_at"])
    df["time_offset"] = (
        df["captured_at"] - df.groupby("video_id")["captured_at"].transform("first")
    ).dt.total_seconds()

    df["d_views"] = df.groupby("video_id")["view_count"].diff()
    df["dt"] = df.groupby("video_id")["time_offset"].diff()
    df["views_per_sec"] = df["d_views"] / df["dt"]
    df.loc[df["dt"] <= 0, "views_per_sec"] = pd.NA

    df["views_per_sec_smooth"] = (
        df.groupby("video_id")["views_per_sec"]
        .transform(lambda x: x.rolling(rolling_window, min_periods=1).mean())
    )

    df["offset_bin_sec"] = (df["time_offset"] // offset_bin_sec) * offset_bin_sec
    avg = (
        df.groupby("offset_bin_sec", as_index=False)[["views_per_sec", "views_per_sec_smooth"]]
        .mean()
        .sort_values("offset_bin_sec")
    )
    avg["offset_hours"] = avg["offset_bin_sec"] / 3600.0
    dt = avg["offset_bin_sec"].diff()
    avg["views_accel_per_sec2"] = avg["views_per_sec_smooth"].diff() / dt
    avg.loc[dt <= 0, "views_accel_per_sec2"] = pd.NA
    return avg


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot average view growth rate vs time offset for videos and shorts."
    )
    parser.add_argument("--video-input", type=Path, default=Path("video_stats.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats.csv"))
    parser.add_argument("--rolling-window", type=int, default=20, help="Rolling window in samples (default: 20)")
    parser.add_argument(
        "--accel-rolling-window",
        type=int,
        default=20,
        help="Additional rolling window for acceleration smoothing (default: 20)",
    )
    parser.add_argument("--offset-bin-sec", type=int, default=30, help="Offset bin in seconds (default: 30)")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("plots/avg_view_growth_rate_vs_offset.png"),
    )
    args = parser.parse_args()

    if args.rolling_window <= 0:
        raise ValueError("--rolling-window must be > 0")
    if args.accel_rolling_window <= 0:
        raise ValueError("--accel-rolling-window must be > 0")
    if args.offset_bin_sec <= 0:
        raise ValueError("--offset-bin-sec must be > 0")

    video_avg = prepare_growth_rates(args.video_input, args.rolling_window, args.offset_bin_sec)
    shorts_avg = prepare_growth_rates(args.shorts_input, args.rolling_window, args.offset_bin_sec)
    video_avg["views_accel_per_sec2_smooth"] = video_avg["views_accel_per_sec2"].rolling(
        args.accel_rolling_window, min_periods=1
    ).mean()
    shorts_avg["views_accel_per_sec2_smooth"] = shorts_avg["views_accel_per_sec2"].rolling(
        args.accel_rolling_window, min_periods=1
    ).mean()

    fig, (ax_rate, ax_accel) = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
    ax_rate.plot(
        video_avg["offset_hours"],
        video_avg["views_per_sec"],
        linewidth=1.0,
        alpha=0.35,
        label="Videos avg (raw derivative)",
    )
    ax_rate.plot(
        shorts_avg["offset_hours"],
        shorts_avg["views_per_sec"],
        linewidth=1.0,
        alpha=0.35,
        label="Shorts avg (raw derivative)",
    )
    ax_rate.plot(
        video_avg["offset_hours"],
        video_avg["views_per_sec_smooth"],
        linewidth=2.2,
        label=f"Videos avg (rolling {args.rolling_window})",
    )
    ax_rate.plot(
        shorts_avg["offset_hours"],
        shorts_avg["views_per_sec_smooth"],
        linewidth=2.2,
        label=f"Shorts avg (rolling {args.rolling_window})",
    )

    ax_rate.set_title("Average View Growth Rate vs Time Offset")
    ax_rate.set_ylabel("Views Per Second")
    ax_rate.grid(True, alpha=0.25)
    ax_rate.legend(loc="best")

    ax_accel.plot(
        video_avg["offset_hours"],
        video_avg["views_accel_per_sec2_smooth"],
        linewidth=2.0,
        label=(
            f"Videos acceleration "
            f"(d rolling-{args.rolling_window} / dt, smooth {args.accel_rolling_window})"
        ),
    )
    ax_accel.plot(
        shorts_avg["offset_hours"],
        shorts_avg["views_accel_per_sec2_smooth"],
        linewidth=2.0,
        label=(
            f"Shorts acceleration "
            f"(d rolling-{args.rolling_window} / dt, smooth {args.accel_rolling_window})"
        ),
    )
    ax_accel.set_title("Acceleration of Smoothed Growth Rate")
    ax_accel.set_xlabel("Hours Since First Seen")
    ax_accel.set_ylabel("Views Per Second²")
    ax_accel.grid(True, alpha=0.25)
    ax_accel.legend(loc="best")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    plt.close(fig)

    print(f"Done. Saved plot to: {args.output.resolve()}")


if __name__ == "__main__":
    main()
