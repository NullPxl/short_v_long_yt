from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import aggregate_competition_timeseries as act
import run_pcmci_causality as rpc


DEFAULT_STEP = "1min"
DEFAULT_METRIC = "avg_new_view_count_per_item"
DEFAULT_MIN_LAG_MINUTES = 5.0
DEFAULT_MAX_LAG_MINUTES = 20.0
METRIC_LABELS = {
    "avg_view_count_per_item": "Average Views Per Item",
    "avg_new_view_count_per_item": "Average New Views Per Item",
    "avg_new_comment_count_per_item": "Average New Comments Per Item",
}


def zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if std == 0 or np.isnan(std):
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - float(series.mean())) / std


def correlation_by_lag(
    transformed_df: pd.DataFrame,
    step: str,
    min_lag_minutes: float,
    max_lag_minutes: float,
) -> pd.DataFrame:
    step_minutes = pd.Timedelta(step).total_seconds() / 60.0
    min_lag_steps = max(1, int(np.ceil(min_lag_minutes / step_minutes)))
    max_lag_steps = max(min_lag_steps, int(np.floor(max_lag_minutes / step_minutes)))

    rows: list[dict[str, float]] = []
    for lag_steps in range(min_lag_steps, max_lag_steps + 1):
        aligned = pd.DataFrame(
            {
                "video": transformed_df["video"],
                "shorts_shifted": transformed_df["shorts"].shift(lag_steps),
            }
        ).dropna()
        rows.append(
            {
                "lag_steps": lag_steps,
                "lag_minutes": lag_steps * step_minutes,
                "correlation": float(aligned["video"].corr(aligned["shorts_shifted"])),
                "n_obs": float(len(aligned)),
            }
        )

    return pd.DataFrame(rows)


def plot_lag_window(
    raw_df: pd.DataFrame,
    transformed_df: pd.DataFrame,
    lag_profile: pd.DataFrame,
    significant_links: pd.DataFrame,
    chosen_lag_steps: int,
    chosen_lag_minutes: float,
    out_path: Path,
    metric: str,
    min_lag_minutes: float,
    max_lag_minutes: float,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw_aligned = pd.DataFrame(
        {
            "timestep": raw_df["timestep"],
            "video": raw_df["video"],
            "shorts_shifted": raw_df["shorts"].shift(chosen_lag_steps),
        }
    ).dropna().reset_index(drop=True)
    transformed_aligned = pd.DataFrame(
        {
            "timestep": transformed_df["timestep"],
            "video": transformed_df["video"],
            "shorts_shifted": transformed_df["shorts"].shift(chosen_lag_steps),
        }
    ).dropna().reset_index(drop=True)

    fig, axes = plt.subplots(3, 1, figsize=(13, 12))
    lag_ax, raw_ax, transformed_ax = axes
    metric_label = METRIC_LABELS.get(metric, metric)

    lag_ax.plot(
        lag_profile["lag_minutes"],
        lag_profile["correlation"],
        linewidth=2.0,
        color="#1f77b4",
    )
    lag_ax.axvline(chosen_lag_minutes, color="#d62728", linestyle="--", linewidth=1.5)
    for _, row in significant_links.iterrows():
        lag_ax.scatter(
            row["lag_minutes"],
            lag_profile.loc[lag_profile["lag_steps"] == row["lag_steps"], "correlation"].iloc[0],
            color="#d62728",
            s=45,
            zorder=3,
        )
    lag_ax.set_xlim(min_lag_minutes, max_lag_minutes)
    lag_ax.set_title("Lag Profile: Shorts Leading Video")
    lag_ax.set_xlabel("Lag Minutes")
    lag_ax.set_ylabel("Pearson Correlation")
    lag_ax.grid(True, alpha=0.25)

    raw_ax.plot(
        raw_aligned["timestep"],
        zscore(raw_aligned["video"]),
        label="Videos (z-scored raw new views)",
        linewidth=1.4,
    )
    raw_ax.plot(
        raw_aligned["timestep"],
        zscore(raw_aligned["shorts_shifted"]),
        label=f"Shorts shifted by {chosen_lag_minutes:.0f} min",
        linewidth=1.4,
    )
    raw_ax.scatter(
        raw_aligned["timestep"],
        zscore(raw_aligned["video"]),
        s=10,
        alpha=0.35,
        color="#1f77b4",
    )
    raw_ax.scatter(
        raw_aligned["timestep"],
        zscore(raw_aligned["shorts_shifted"]),
        s=10,
        alpha=0.35,
        color="#ff7f0e",
    )
    raw_ax.set_title(f"Raw {metric_label} Curves Aligned at the Selected Lag")
    raw_ax.set_ylabel("Z-Score")
    raw_ax.grid(True, alpha=0.25)
    raw_ax.legend()

    transformed_ax.plot(
        transformed_aligned["timestep"],
        zscore(transformed_aligned["video"]),
        label="Videos (z-scored PCMCI input)",
        linewidth=1.4,
    )
    transformed_ax.plot(
        transformed_aligned["timestep"],
        zscore(transformed_aligned["shorts_shifted"]),
        label=f"Shorts shifted by {chosen_lag_minutes:.0f} min",
        linewidth=1.4,
    )
    transformed_ax.scatter(
        transformed_aligned["timestep"],
        zscore(transformed_aligned["video"]),
        s=10,
        alpha=0.35,
        color="#1f77b4",
    )
    transformed_ax.scatter(
        transformed_aligned["timestep"],
        zscore(transformed_aligned["shorts_shifted"]),
        s=10,
        alpha=0.35,
        color="#ff7f0e",
    )
    transformed_ax.set_title(f"Transformed {metric_label} Curves Aligned at the Selected Lag")
    transformed_ax.set_xlabel("Captured Timestep (UTC)")
    transformed_ax.set_ylabel("Z-Score")
    transformed_ax.grid(True, alpha=0.25)
    transformed_ax.legend()

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Plot the lag window around a PCMCI finding, including the lag profile "
            "and aligned video/shorts curves."
        )
    )
    parser.add_argument("--video-input", type=Path, default=Path("video_stats_combined.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats_combined.csv"))
    parser.add_argument("--step", default=DEFAULT_STEP)
    parser.add_argument("--metric", default=DEFAULT_METRIC)
    parser.add_argument("--category", default="")
    parser.add_argument("--min-lag-minutes", type=float, default=DEFAULT_MIN_LAG_MINUTES)
    parser.add_argument("--max-lag-minutes", type=float, default=DEFAULT_MAX_LAG_MINUTES)
    parser.add_argument(
        "--links-input",
        type=Path,
        default=Path("final_causality") / "pcmci_links_avg_new_view_count_per_item_1min.csv",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("final_causality") / "pcmci_lag_window_avg_new_view_count_per_item_1min.png",
    )
    args = parser.parse_args()

    suffix = f"{args.metric}_{args.step}"
    if args.category:
        suffix = f"{args.category}_{suffix}"

    raw_df = rpc.build_joint_series(args.video_input, args.shorts_input, args.step, args.metric, args.category)
    transformed_input_path = Path("causality") / f"pcmci_input_{suffix}.csv"
    if not transformed_input_path.exists():
        transformed_input_path = Path("final_causality") / f"pcmci_input_{suffix}.csv"
    if transformed_input_path.exists():
        transformed_df = pd.read_csv(transformed_input_path, parse_dates=["timestep"])
    else:
        chosen_transform, _ = rpc.evaluate_transforms(raw_df)
        transformed_df = rpc.apply_transform(raw_df, chosen_transform)

    lag_profile = correlation_by_lag(
        transformed_df=transformed_df,
        step=args.step,
        min_lag_minutes=args.min_lag_minutes,
        max_lag_minutes=args.max_lag_minutes,
    )

    significant_links = pd.DataFrame()
    if args.links_input.exists():
        links_df = pd.read_csv(args.links_input)
        significant_links = links_df.loc[
            links_df["cross_series"]
            & links_df["significant"]
            & (links_df["source"] == "shorts")
            & (links_df["target"] == "video")
            & (links_df["lag_minutes"] >= args.min_lag_minutes)
            & (links_df["lag_minutes"] <= args.max_lag_minutes)
        ].copy()

    if significant_links.empty:
        best_row = lag_profile.loc[lag_profile["correlation"].abs().idxmax()]
        chosen_lag_steps = int(best_row["lag_steps"])
        chosen_lag_minutes = float(best_row["lag_minutes"])
    else:
        best_row = significant_links.sort_values("q_value").iloc[0]
        chosen_lag_steps = int(best_row["lag_steps"])
        chosen_lag_minutes = float(best_row["lag_minutes"])

    plot_lag_window(
        raw_df=raw_df,
        transformed_df=transformed_df,
        lag_profile=lag_profile,
        significant_links=significant_links,
        chosen_lag_steps=chosen_lag_steps,
        chosen_lag_minutes=chosen_lag_minutes,
        out_path=args.output_path,
        metric=args.metric,
        min_lag_minutes=args.min_lag_minutes,
        max_lag_minutes=args.max_lag_minutes,
    )

    print(f"Wrote: {args.output_path.resolve()}")
    print(f"Chosen lag: {chosen_lag_steps} steps / {chosen_lag_minutes:.0f} minutes")


if __name__ == "__main__":
    main()
