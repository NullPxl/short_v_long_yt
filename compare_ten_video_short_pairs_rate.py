from __future__ import annotations

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REQUIRED_COLUMNS = {"captured_at", "video_id", "view_count"}


def load_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {sorted(missing)}")

    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df = df.dropna(subset=["captured_at", "video_id", "view_count"])
    return df


def ordered_ids_by_first_seen(df: pd.DataFrame) -> list[str]:
    first_seen_df = (
        df.groupby("video_id", as_index=False)["captured_at"]
        .min()
        .sort_values("captured_at")
    )
    return [str(x) for x in first_seen_df["video_id"].tolist()]


def series_for_id(df: pd.DataFrame, item_id: str) -> pd.DataFrame:
    s = df[df["video_id"] == item_id].sort_values("captured_at").copy()
    if s.empty:
        return s
    s["hours_since_first_seen"] = (s["captured_at"] - s["captured_at"].min()).dt.total_seconds() / 3600.0
    dt_hours = s["hours_since_first_seen"].diff()
    s["view_gain_per_hour"] = s["view_count"].diff() / dt_hours
    s = s[dt_hours > 0].copy()
    return s


def choose_pairs(video_ids: list[str], short_ids: list[str], num_pairs: int) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    used_short_ids: set[str] = set()

    for v_id in video_ids:
        chosen_short = None
        for s_id in short_ids:
            if s_id in used_short_ids:
                continue
            if s_id != v_id:
                chosen_short = s_id
                break
        if chosen_short is None:
            for s_id in short_ids:
                if s_id not in used_short_ids:
                    chosen_short = s_id
                    break
        if chosen_short is None:
            break
        pairs.append((v_id, chosen_short))
        used_short_ids.add(chosen_short)
        if len(pairs) >= num_pairs:
            break

    return pairs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create one PNG with many 1v1 view-rate comparisons (video vs short)."
    )
    parser.add_argument("--video-input", type=Path, default=Path("video_stats.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats.csv"))
    parser.add_argument("--num-pairs", type=int, default=10, help="Number of video/short pairings to plot")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("plots/ten_video_vs_short_view_rate_pairs.png"),
    )
    args = parser.parse_args()

    if args.num_pairs <= 0:
        raise ValueError("--num-pairs must be > 0")

    video_df = load_csv(args.video_input)
    shorts_df = load_csv(args.shorts_input)
    video_ids = ordered_ids_by_first_seen(video_df)
    short_ids = ordered_ids_by_first_seen(shorts_df)

    max_pairs = min(len(video_ids), len(short_ids))
    target_pairs = min(args.num_pairs, max_pairs)
    pairs = choose_pairs(video_ids, short_ids, target_pairs)
    if not pairs:
        raise ValueError("Could not find any valid video/short pairs to plot.")

    n = len(pairs)
    ncols = 2
    nrows = math.ceil(n / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, max(3.4 * nrows, 5.5)))
    axes_list = axes.flatten() if hasattr(axes, "flatten") else [axes]

    for idx, (video_id, short_id) in enumerate(pairs):
        ax = axes_list[idx]
        v = series_for_id(video_df, video_id)
        s = series_for_id(shorts_df, short_id)

        if v.empty or s.empty:
            ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(f"Pair {idx + 1}: {video_id} vs {short_id}")
            continue

        ax.plot(v["hours_since_first_seen"], v["view_gain_per_hour"], linewidth=1.6, label=f"Video: {video_id}")
        ax.plot(s["hours_since_first_seen"], s["view_gain_per_hour"], linewidth=1.6, label=f"Short: {short_id}")
        ax.set_title(f"Pair {idx + 1}")
        ax.set_xlabel("Hours Since First Seen")
        ax.set_ylabel("Views Gained Per Hour")
        ax.grid(True, alpha=0.25)
        ax.legend(fontsize=8, loc="best")

    for idx in range(n, len(axes_list)):
        axes_list[idx].axis("off")

    fig.suptitle("View Growth Rate Over Time: 1v1 Video vs Short Pairings", y=0.995)
    fig.tight_layout()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=150)
    plt.close(fig)

    print(f"Done. Saved {n} pair plots to {args.output.resolve()}")


if __name__ == "__main__":
    main()
