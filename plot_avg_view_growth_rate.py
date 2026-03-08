from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")

REQUIRED_COLUMNS = {"captured_at", "video_id", "view_count", "like_count", "comment_count"}


def prepare_growth_rates(
    csv_path: Path,
    metric_col: str,
    rolling_window: int,
    offset_bin_sec: int,
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")
    df = df.dropna(subset=["captured_at", "video_id", metric_col])
    df = df.sort_values(["video_id", "captured_at"])

    df["time_offset"] = (
        df["captured_at"] - df.groupby("video_id")["captured_at"].transform("first")
    ).dt.total_seconds()
    df["d_metric"] = df.groupby("video_id")[metric_col].diff()
    df["dt"] = df.groupby("video_id")["time_offset"].diff()
    df["metric_per_sec"] = df["d_metric"] / df["dt"]
    df.loc[df["dt"] <= 0, "metric_per_sec"] = pd.NA

    df["metric_per_sec_smooth"] = (
        df.groupby("video_id")["metric_per_sec"]
        .transform(lambda x: x.rolling(rolling_window, min_periods=1).mean())
    )

    df["offset_bin_sec"] = (df["time_offset"] // offset_bin_sec) * offset_bin_sec
    avg = (
        df.groupby("offset_bin_sec", as_index=False)[["metric_per_sec", "metric_per_sec_smooth"]]
        .mean()
        .sort_values("offset_bin_sec")
    )
    avg["offset_hours"] = avg["offset_bin_sec"] / 3600.0
    dt = avg["offset_bin_sec"].diff()
    avg["metric_accel_per_sec2"] = avg["metric_per_sec_smooth"].diff() / dt
    avg.loc[dt <= 0, "metric_accel_per_sec2"] = pd.NA
    return avg


def plot_metric(
    metric_label: str,
    video_avg: pd.DataFrame,
    shorts_avg: pd.DataFrame,
    rolling_window: int,
    accel_rolling_window: int,
    output_path: Path,
) -> None:
    video_avg = video_avg.copy()
    shorts_avg = shorts_avg.copy()
    video_avg["metric_accel_per_sec2_smooth"] = video_avg["metric_accel_per_sec2"].rolling(
        accel_rolling_window, min_periods=1
    ).mean()
    shorts_avg["metric_accel_per_sec2_smooth"] = shorts_avg["metric_accel_per_sec2"].rolling(
        accel_rolling_window, min_periods=1
    ).mean()

    fig, (ax_rate, ax_accel) = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
    ax_rate.plot(
        video_avg["offset_hours"],
        video_avg["metric_per_sec"],
        linewidth=1.0,
        alpha=0.35,
        label="Videos avg (raw derivative)",
    )
    ax_rate.plot(
        shorts_avg["offset_hours"],
        shorts_avg["metric_per_sec"],
        linewidth=1.0,
        alpha=0.35,
        label="Shorts avg (raw derivative)",
    )
    ax_rate.plot(
        video_avg["offset_hours"],
        video_avg["metric_per_sec_smooth"],
        linewidth=2.2,
        label=f"Videos avg (rolling {rolling_window})",
    )
    ax_rate.plot(
        shorts_avg["offset_hours"],
        shorts_avg["metric_per_sec_smooth"],
        linewidth=2.2,
        label=f"Shorts avg (rolling {rolling_window})",
    )
    ax_rate.set_title(f"Average {metric_label} Growth Rate vs Time Offset")
    ax_rate.set_ylabel(f"{metric_label} Per Second")
    ax_rate.grid(True, alpha=0.25)
    ax_rate.legend(loc="best")

    ax_accel.plot(
        video_avg["offset_hours"],
        video_avg["metric_accel_per_sec2_smooth"],
        linewidth=2.0,
        label=(
            f"Videos acceleration "
            f"(d rolling-{rolling_window} / dt, smooth {accel_rolling_window})"
        ),
    )
    ax_accel.plot(
        shorts_avg["offset_hours"],
        shorts_avg["metric_accel_per_sec2_smooth"],
        linewidth=2.0,
        label=(
            f"Shorts acceleration "
            f"(d rolling-{rolling_window} / dt, smooth {accel_rolling_window})"
        ),
    )
    ax_accel.set_title("Acceleration of Smoothed Growth Rate")
    ax_accel.set_xlabel("Hours Since First Seen")
    ax_accel.set_ylabel(f"{metric_label} Per Second^2")
    ax_accel.grid(True, alpha=0.25)
    ax_accel.legend(loc="best")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot average growth-rate and acceleration vs offset for views, likes, and comments."
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
    parser.add_argument("--output-dir", type=Path, default=Path("plots"))
    args = parser.parse_args()

    if args.rolling_window <= 0:
        raise ValueError("--rolling-window must be > 0")
    if args.accel_rolling_window <= 0:
        raise ValueError("--accel-rolling-window must be > 0")
    if args.offset_bin_sec <= 0:
        raise ValueError("--offset-bin-sec must be > 0")

    metric_specs = [
        ("view_count", "Views", "avg_view_growth_rate_vs_offset.png"),
        ("like_count", "Likes", "avg_like_growth_rate_vs_offset.png"),
        ("comment_count", "Comments", "avg_comment_growth_rate_vs_offset.png"),
    ]
    for metric_col, metric_label, filename in metric_specs:
        video_avg = prepare_growth_rates(
            args.video_input,
            metric_col,
            args.rolling_window,
            args.offset_bin_sec,
        )
        shorts_avg = prepare_growth_rates(
            args.shorts_input,
            metric_col,
            args.rolling_window,
            args.offset_bin_sec,
        )
        plot_metric(
            metric_label=metric_label,
            video_avg=video_avg,
            shorts_avg=shorts_avg,
            rolling_window=args.rolling_window,
            accel_rolling_window=args.accel_rolling_window,
            output_path=args.output_dir / filename,
        )

    print(f"Done. Saved 3 plots to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
