from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import aggregate_competition_timeseries as act


DEFAULT_STEP = "1min"
DEFAULT_MAX_AGE_MINUTES = 180
DEFAULT_MIN_ITEMS_PER_AGE = 5


def build_aligned_attention(df: pd.DataFrame, step: str) -> pd.DataFrame:
    step_seconds = pd.Timedelta(step).total_seconds()
    aligned = df.copy()
    first_seen = aligned.groupby("video_id")["captured_at"].transform("min")
    aligned["age_step"] = (
        ((aligned["captured_at"] - first_seen).dt.total_seconds() / step_seconds)
        .round()
        .astype(int)
    )
    aligned["age_minutes"] = aligned["age_step"] * step_seconds / 60.0

    per_item = (
        aligned.groupby(["video_id", "age_step", "age_minutes"], as_index=False)
        .agg(new_view_count=("new_view_count", "sum"))
        .sort_values(["video_id", "age_step"])
    )

    peak = per_item.groupby("video_id")["new_view_count"].transform("max")
    peak = peak.replace(0, np.nan)
    per_item["normalized_attention"] = (per_item["new_view_count"] / peak).fillna(0.0)
    per_item["cumulative_views"] = per_item.groupby("video_id")["new_view_count"].cumsum()
    return per_item


def build_peak_aligned_decay(per_item: pd.DataFrame, step: str) -> pd.DataFrame:
    step_minutes = pd.Timedelta(step).total_seconds() / 60.0
    peak_idx = per_item.groupby("video_id")["normalized_attention"].idxmax()
    peak_rows = (
        per_item.loc[peak_idx, ["video_id", "age_step"]]
        .rename(columns={"age_step": "peak_age_step"})
        .reset_index(drop=True)
    )
    peak_aligned = per_item.merge(peak_rows, on="video_id", how="left")
    peak_aligned["steps_since_peak"] = peak_aligned["age_step"] - peak_aligned["peak_age_step"]
    peak_aligned = peak_aligned.loc[peak_aligned["steps_since_peak"] >= 0].copy()
    peak_aligned["minutes_since_peak"] = peak_aligned["steps_since_peak"] * step_minutes
    return peak_aligned


def summarize_decay(
    per_item: pd.DataFrame,
    age_column: str,
    max_age_minutes: int,
    min_items_per_age: int,
) -> pd.DataFrame:
    summary = (
        per_item.loc[per_item[age_column] <= max_age_minutes]
        .groupby(age_column, as_index=False)
        .agg(
            mean_normalized_attention=("normalized_attention", "mean"),
            median_normalized_attention=("normalized_attention", "median"),
            mean_new_view_count=("new_view_count", "mean"),
            items_observed=("video_id", "nunique"),
        )
        .sort_values(age_column)
    )
    summary["eligible_for_fit"] = summary["items_observed"] >= min_items_per_age
    return summary


def fit_beta_grid(
    age_minutes: np.ndarray,
    values: np.ndarray,
    model: str,
) -> tuple[float, np.ndarray, float]:
    grid = np.linspace(0.0001, 1.0, 10000)
    best_beta = float(grid[0])
    best_pred = np.zeros_like(values)
    best_sse = float("inf")

    for beta in grid:
        if model == "exp":
            pred = np.exp(-beta * age_minutes)
        elif model == "quad":
            pred = 1.0 / (1.0 + beta * age_minutes)
        else:
            raise ValueError(f"Unknown model: {model}")

        sse = float(np.square(values - pred).sum())
        if sse < best_sse:
            best_sse = sse
            best_beta = float(beta)
            best_pred = pred

    return best_beta, best_pred, best_sse


def fit_decay_models(summary: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    fit_df = summary.loc[summary["eligible_for_fit"]].copy()
    x_col = "minutes_since_peak" if "minutes_since_peak" in fit_df.columns else "age_minutes"
    fit_df = fit_df.loc[fit_df[x_col] >= 0]
    x = fit_df[x_col].to_numpy(dtype=float)
    y = fit_df["mean_normalized_attention"].to_numpy(dtype=float)

    exp_beta, exp_pred, exp_sse = fit_beta_grid(x, y, "exp")
    quad_beta, quad_pred, quad_sse = fit_beta_grid(x, y, "quad")

    fit_df["exp_fit"] = exp_pred
    fit_df["quad_fit"] = quad_pred
    fit_df["exp_abs_error"] = np.abs(fit_df["mean_normalized_attention"] - fit_df["exp_fit"])
    fit_df["quad_abs_error"] = np.abs(fit_df["mean_normalized_attention"] - fit_df["quad_fit"])

    result = pd.DataFrame(
        [
            {"model": "exp", "beta": exp_beta, "sse": exp_sse},
            {"model": "quad", "beta": quad_beta, "sse": quad_sse},
        ]
    )
    return fit_df, result


def plot_decay(
    video_summary: pd.DataFrame,
    shorts_summary: pd.DataFrame,
    video_fit: pd.DataFrame,
    shorts_fit: pd.DataFrame,
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    avg_ax, fit_ax = axes

    x_col = "minutes_since_peak"

    avg_ax.plot(
        video_summary[x_col],
        video_summary["mean_normalized_attention"],
        label="Videos",
        linewidth=2.0,
    )
    avg_ax.plot(
        shorts_summary[x_col],
        shorts_summary["mean_normalized_attention"],
        label="Shorts",
        linewidth=2.0,
    )
    avg_ax.set_title("Average Normalized Attention Decay After Peak")
    avg_ax.set_ylabel("Mean Normalized New Views")
    avg_ax.grid(True, alpha=0.25)
    avg_ax.legend()

    fit_ax.plot(
        video_fit[x_col],
        video_fit["mean_normalized_attention"],
        label="Videos empirical",
        linewidth=2.0,
        color="#1f77b4",
    )
    fit_ax.plot(
        video_fit[x_col],
        video_fit["quad_fit"],
        label="Videos quadratic fit",
        linewidth=1.6,
        linestyle="--",
        color="#1f77b4",
    )
    fit_ax.plot(
        shorts_fit[x_col],
        shorts_fit["mean_normalized_attention"],
        label="Shorts empirical",
        linewidth=2.0,
        color="#ff7f0e",
    )
    fit_ax.plot(
        shorts_fit[x_col],
        shorts_fit["quad_fit"],
        label="Shorts quadratic fit",
        linewidth=1.6,
        linestyle="--",
        color="#ff7f0e",
    )
    fit_ax.set_title("Quadratic Decay Comparison: dA/dt = -beta A^2")
    fit_ax.set_xlabel("Minutes Since Peak")
    fit_ax.set_ylabel("Mean Normalized New Views")
    fit_ax.grid(True, alpha=0.25)
    fit_ax.legend(ncol=2)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Align shorts and regular videos by age, compare their normalized attention "
            "decay curves, and fit exponential/quadratic decay models."
        )
    )
    parser.add_argument("--video-input", type=Path, default=Path("video_stats.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats.csv"))
    parser.add_argument("--step", default=DEFAULT_STEP)
    parser.add_argument("--max-age-minutes", type=int, default=DEFAULT_MAX_AGE_MINUTES)
    parser.add_argument("--min-items-per-age", type=int, default=DEFAULT_MIN_ITEMS_PER_AGE)
    parser.add_argument("--output-dir", type=Path, default=Path("decay"))
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    video_raw = act.load_and_prepare(args.video_input)
    shorts_raw = act.load_and_prepare(args.shorts_input)

    video_per_item = build_aligned_attention(video_raw, args.step)
    shorts_per_item = build_aligned_attention(shorts_raw, args.step)
    video_peak_decay = build_peak_aligned_decay(video_per_item, args.step)
    shorts_peak_decay = build_peak_aligned_decay(shorts_per_item, args.step)

    video_summary = summarize_decay(
        video_peak_decay,
        age_column="minutes_since_peak",
        max_age_minutes=args.max_age_minutes,
        min_items_per_age=args.min_items_per_age,
    )
    shorts_summary = summarize_decay(
        shorts_peak_decay,
        age_column="minutes_since_peak",
        max_age_minutes=args.max_age_minutes,
        min_items_per_age=args.min_items_per_age,
    )

    video_fit, video_models = fit_decay_models(video_summary)
    shorts_fit, shorts_models = fit_decay_models(shorts_summary)

    video_summary_out = args.output_dir / "video_decay_curve.csv"
    shorts_summary_out = args.output_dir / "shorts_decay_curve.csv"
    model_summary_out = args.output_dir / "decay_model_comparison.csv"
    plot_out = Path("plots") / "decay_comparison_video_vs_shorts.png"

    video_summary.to_csv(video_summary_out, index=False)
    shorts_summary.to_csv(shorts_summary_out, index=False)

    model_summary = pd.concat(
        [
            video_models.assign(format="videos"),
            shorts_models.assign(format="shorts"),
        ],
        ignore_index=True,
    )[["format", "model", "beta", "sse"]]
    model_summary.to_csv(model_summary_out, index=False)

    plot_decay(video_summary, shorts_summary, video_fit, shorts_fit, plot_out)

    print(f"Wrote: {video_summary_out.resolve()}")
    print(f"Wrote: {shorts_summary_out.resolve()}")
    print(f"Wrote: {model_summary_out.resolve()}")
    print(f"Wrote: {plot_out.resolve()}")
    print(
        "Quadratic beta comparison: "
        f"videos={video_models.loc[video_models['model'] == 'quad', 'beta'].iloc[0]:.4f}, "
        f"shorts={shorts_models.loc[shorts_models['model'] == 'quad', 'beta'].iloc[0]:.4f}"
    )


if __name__ == "__main__":
    main()
