from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REQUIRED_COLUMNS = {"captured_at", "video_id", "view_count"}


def load_single_series(
    csv_path: Path,
    item_id: str | None,
    exclude_id: str | None = None,
) -> tuple[pd.DataFrame, str]:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df = df.dropna(subset=["captured_at", "video_id", "view_count"])

    if item_id is None:
        first_seen_df = (
            df.groupby("video_id", as_index=False)["captured_at"]
            .min()
            .sort_values("captured_at")
        )
        ordered_ids = [str(x) for x in first_seen_df["video_id"].tolist()]
        if exclude_id is not None:
            filtered_ids = [x for x in ordered_ids if x != exclude_id]
            if filtered_ids:
                ordered_ids = filtered_ids
        item_id = ordered_ids[0]

    series = df[df["video_id"] == item_id].sort_values("captured_at").copy()
    if series.empty:
        raise ValueError(f"video_id '{item_id}' not found in {csv_path}")

    series["hours_since_first_seen"] = (
        series["captured_at"] - series["captured_at"].min()
    ).dt.total_seconds() / 3600.0
    dt_hours = series["hours_since_first_seen"].diff()
    d_views = series["view_count"].diff()
    series["view_gain_per_hour"] = d_views / dt_hours
    series = series[dt_hours > 0].copy()

    return series, item_id


def make_pct_change_15m(series: pd.DataFrame) -> pd.DataFrame:
    s = (
        series.set_index("captured_at")[["view_count"]]
        .sort_index()
        .resample("15min")
        .last()
        .dropna()
        .reset_index()
    )
    if s.empty:
        s["hours_since_first_seen"] = pd.Series(dtype=float)
        s["view_pct_change_15m"] = pd.Series(dtype=float)
        return s

    s["hours_since_first_seen"] = (s["captured_at"] - s["captured_at"].min()).dt.total_seconds() / 3600.0
    prev = s["view_count"].shift(1)
    s["view_pct_change_15m"] = ((s["view_count"] - prev) / prev) * 100.0
    s = s[prev > 0].copy()
    return s


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compare view growth-rate over time (views/hour) for one regular video and one short, "
            "aligned to offset from first seen."
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
        "--video-id",
        type=str,
        default=None,
        help="Regular-video id to compare (default: earliest first-seen in video CSV)",
    )
    parser.add_argument(
        "--short-id",
        type=str,
        default=None,
        help="Short id to compare (default: earliest first-seen in shorts CSV)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("plots/one_video_vs_one_short_view_rate.png"),
        help="Output PNG path",
    )
    args = parser.parse_args()

    video_series, resolved_video_id = load_single_series(args.video_input, args.video_id)
    short_series, resolved_short_id = load_single_series(
        args.shorts_input,
        args.short_id,
        exclude_id=resolved_video_id if args.short_id is None else None,
    )
    video_pct_15m = make_pct_change_15m(video_series)
    short_pct_15m = make_pct_change_15m(short_series)

    args.output.parent.mkdir(parents=True, exist_ok=True)

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(14, 6))
    ax_left.plot(
        video_series["hours_since_first_seen"],
        video_series["view_gain_per_hour"],
        linewidth=2.0,
        label=f"Video: {resolved_video_id}",
    )
    ax_left.plot(
        short_series["hours_since_first_seen"],
        short_series["view_gain_per_hour"],
        linewidth=2.0,
        label=f"Short: {resolved_short_id}",
    )
    ax_left.set_title("Raw View Growth Rate")
    ax_left.set_xlabel("Hours Since First Seen")
    ax_left.set_ylabel("Views Gained Per Hour")
    ax_left.grid(True, alpha=0.25)
    ax_left.legend(loc="best")

    ax_right.plot(
        video_pct_15m["hours_since_first_seen"],
        video_pct_15m["view_pct_change_15m"],
        linewidth=2.0,
        label=f"Video: {resolved_video_id}",
    )
    ax_right.plot(
        short_pct_15m["hours_since_first_seen"],
        short_pct_15m["view_pct_change_15m"],
        linewidth=2.0,
        label=f"Short: {resolved_short_id}",
    )
    ax_right.set_title("% View Change Over Previous 15 Min")
    ax_right.set_xlabel("Hours Since First Seen")
    ax_right.set_ylabel("Percent Change (%)")
    ax_right.grid(True, alpha=0.25)
    ax_right.legend(loc="best")

    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    plt.close(fig)

    print(
        "Done. Saved comparison plot to "
        f"{args.output.resolve()} (video_id={resolved_video_id}, short_id={resolved_short_id})"
    )


if __name__ == "__main__":
    main()
