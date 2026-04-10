from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D

matplotlib.use("Agg")

REQUIRED_COLUMNS = {"captured_at", "video_id", "view_count", "category"}
CATEGORY_COLORS = {
    "basketball": "#1f77b4",
    "formula1": "#d62728",
    "chess": "#2ca02c",
}
TYPE_MARKERS = {
    "videos": "o",
    "shorts": "s",
}


def extract_peak_points(
    csv_path: Path,
    item_type: str,
    rolling_window: int,
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df["category"] = df["category"].astype(str).str.strip()
    df = df.dropna(subset=["captured_at", "video_id", "view_count", "category"])
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

    peaks: list[dict[str, float | str]] = []
    for video_id, group in df.groupby("video_id", sort=False):
        category = group["category"].iloc[0]
        peak_frame = group[["time_offset_sec", "views_per_sec_smooth"]].dropna()
        if peak_frame.empty:
            continue
        idx = peak_frame["views_per_sec_smooth"].idxmax()
        peaks.append(
            {
                "video_id": video_id,
                "category": category,
                "item_type": item_type,
                "peak_time_hours": float(peak_frame.loc[idx, "time_offset_sec"]) / 3600.0,
                "peak_views_per_sec": float(peak_frame.loc[idx, "views_per_sec_smooth"]),
            }
        )

    return pd.DataFrame(peaks)


def plot_peak_points(peaks: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 7))

    for item_type, marker in TYPE_MARKERS.items():
        subset = peaks[peaks["item_type"] == item_type]
        if subset.empty:
            continue
        colors = subset["category"].map(lambda x: CATEGORY_COLORS.get(x, "#7f7f7f"))
        ax.scatter(
            subset["peak_time_hours"],
            subset["peak_views_per_sec"],
            c=colors,
            marker=marker,
            alpha=0.8,
            s=42,
            edgecolors="none",
        )

    category_handles = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            label=category,
            markerfacecolor=color,
            markersize=8,
        )
        for category, color in CATEGORY_COLORS.items()
    ]
    type_handles = [
        Line2D(
            [0],
            [0],
            marker=marker,
            color="#444444",
            linestyle="None",
            label=item_type.capitalize(),
            markersize=8,
        )
        for item_type, marker in TYPE_MARKERS.items()
    ]

    legend_categories = ax.legend(
        handles=category_handles,
        title="Category",
        loc="upper right",
    )
    ax.add_artist(legend_categories)
    ax.legend(handles=type_handles, title="Type", loc="upper center")

    ax.set_title("Peak View Growth Points by Category and Video Type")
    ax.set_xlabel("Time to Peak (hours since first seen)")
    ax.set_ylabel("Peak View Growth Rate (views/sec, smoothed)")
    ax.grid(True, alpha=0.25)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot peak view-growth points with category by color and videos-vs-shorts by shape."
    )
    parser.add_argument("--video-input", type=Path, default=Path("video_stats_combined.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats_combined.csv"))
    parser.add_argument("--rolling-window", type=int, default=20)
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("final_plots") / "peak_points_views_by_category_and_type.png",
    )
    args = parser.parse_args()

    if args.rolling_window <= 0:
        raise ValueError("--rolling-window must be > 0")

    video_peaks = extract_peak_points(args.video_input, "videos", args.rolling_window)
    shorts_peaks = extract_peak_points(args.shorts_input, "shorts", args.rolling_window)
    peaks = pd.concat([video_peaks, shorts_peaks], ignore_index=True)
    plot_peak_points(peaks, args.output_path)

    print(f"Saved plot to: {args.output_path.resolve()}")


if __name__ == "__main__":
    main()
