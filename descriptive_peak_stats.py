from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = {"captured_at", "video_id", "view_count"}


def load_and_prepare(csv_path: Path, rolling_window: int) -> pd.DataFrame:
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
    df.loc[df["dt"] <= 0, "views_per_sec"] = np.nan

    df["views_per_sec_smooth"] = (
        df.groupby("video_id")["views_per_sec"]
        .transform(lambda x: x.rolling(rolling_window, min_periods=1).mean())
    )
    return df


def compute_per_item_peak_stats(
    df: pd.DataFrame,
    drop_frac: float,
    sustain_points: int,
) -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []

    for video_id, g in df.groupby("video_id", sort=False):
        g = g[["time_offset_sec", "views_per_sec_smooth"]].dropna().reset_index(drop=True)
        if g.empty:
            continue

        peak_pos = int(g["views_per_sec_smooth"].idxmax())
        peak_rate = float(g.loc[peak_pos, "views_per_sec_smooth"])
        peak_time_sec = float(g.loc[peak_pos, "time_offset_sec"])

        threshold = drop_frac * peak_rate
        drop_time_sec = np.nan
        for i in range(peak_pos + 1, len(g) - sustain_points + 1):
            window = g.loc[i : i + sustain_points - 1, "views_per_sec_smooth"]
            if (window < threshold).all():
                drop_time_sec = float(g.loc[i, "time_offset_sec"])
                break

        duration_sec = np.nan if np.isnan(drop_time_sec) else (drop_time_sec - peak_time_sec)
        rows.append(
            {
                "video_id": video_id,
                "peak_rate_views_per_sec": peak_rate,
                "time_to_peak_sec": peak_time_sec,
                "drop_time_sec": drop_time_sec,
                "peak_duration_sec": duration_sec,
            }
        )

    return pd.DataFrame(rows)


def summarize(stats_df: pd.DataFrame) -> dict[str, float]:
    t_peak_min = stats_df["time_to_peak_sec"] / 60.0
    dur_min = stats_df["peak_duration_sec"] / 60.0
    return {
        "n_items": int(len(stats_df)),
        "time_to_peak_mean_min": float(t_peak_min.mean()),
        "time_to_peak_median_min": float(t_peak_min.median()),
        "peak_duration_mean_min": float(dur_min.mean()),
        "peak_duration_median_min": float(dur_min.median()),
        "n_with_detected_drop": int(dur_min.notna().sum()),
    }


def print_summary(label: str, summary: dict[str, float]) -> None:
    print(f"\n[{label}]")
    print(f"n_items: {summary['n_items']}")
    print(
        "time_to_peak_min (mean / median): "
        f"{summary['time_to_peak_mean_min']:.2f} / {summary['time_to_peak_median_min']:.2f}"
    )
    print(
        "peak_duration_min (mean / median): "
        f"{summary['peak_duration_mean_min']:.2f} / {summary['peak_duration_median_min']:.2f}"
    )
    print(f"n_with_detected_drop: {summary['n_with_detected_drop']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Descriptive peak stats for videos vs shorts using smoothed view growth rate."
    )
    parser.add_argument("--video-input", type=Path, default=Path("video_stats.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats.csv"))
    parser.add_argument("--rolling-window", type=int, default=20)
    parser.add_argument(
        "--drop-frac",
        type=float,
        default=0.5,
        help="Clear-drop threshold as fraction of peak rate (default: 0.5 = 50%% of peak).",
    )
    parser.add_argument(
        "--sustain-points",
        type=int,
        default=6,
        help="Require this many consecutive points below threshold to confirm drop (default: 6).",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("plots"))
    args = parser.parse_args()

    if args.rolling_window <= 0:
        raise ValueError("--rolling-window must be > 0")
    if not (0 < args.drop_frac < 1):
        raise ValueError("--drop-frac must be between 0 and 1")
    if args.sustain_points <= 0:
        raise ValueError("--sustain-points must be > 0")

    print("Definitions:")
    print("- Peak: max smoothed growth-rate point for each item.")
    print("- Smoothed growth rate: rolling mean of views_per_sec with window = rolling-window.")
    print(
        "- Peak duration: time from peak until first sustained clear drop, where 'clear drop' means "
        f"rate < {args.drop_frac:.2f} * peak for {args.sustain_points} consecutive samples."
    )

    video_df = load_and_prepare(args.video_input, args.rolling_window)
    shorts_df = load_and_prepare(args.shorts_input, args.rolling_window)

    video_stats = compute_per_item_peak_stats(video_df, args.drop_frac, args.sustain_points)
    shorts_stats = compute_per_item_peak_stats(shorts_df, args.drop_frac, args.sustain_points)

    video_summary = summarize(video_stats)
    shorts_summary = summarize(shorts_stats)

    print_summary("Videos", video_summary)
    print_summary("Shorts", shorts_summary)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    video_stats.to_csv(args.output_dir / "video_peak_stats_per_item.csv", index=False)
    shorts_stats.to_csv(args.output_dir / "shorts_peak_stats_per_item.csv", index=False)
    pd.DataFrame(
        [
            {"group": "videos", **video_summary},
            {"group": "shorts", **shorts_summary},
        ]
    ).to_csv(args.output_dir / "peak_stats_summary.csv", index=False)

    print(f"\nSaved CSV outputs to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
