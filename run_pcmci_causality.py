from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.tools.sm_exceptions import InterpolationWarning
from statsmodels.tsa.stattools import adfuller, kpss
from tigramite import data_processing as pp
from tigramite.independence_tests.parcorr import ParCorr
from tigramite.pcmci import PCMCI

import aggregate_competition_timeseries as act


DEFAULT_STEP = "5min"
DEFAULT_METRIC = "avg_new_view_count_per_item"
DEFAULT_TAU_MAX = 12
DEFAULT_PC_ALPHA = 0.2
DEFAULT_ALPHA_LEVEL = 0.05
TRANSFORM_ORDER = ("logdiff", "diff", "raw")
METRIC_LABELS = {
    "total_view_count": "Total Views",
    "new_view_count": "New Views",
    "new_comment_count": "New Comments",
    "avg_view_count_per_item": "Average Views Per Item",
    "avg_new_view_count_per_item": "Average New Views Per Item",
    "avg_new_comment_count_per_item": "Average New Comments Per Item",
}


def build_joint_series(
    video_input: Path,
    shorts_input: Path,
    step: str,
    metric: str,
    category: str = "",
) -> pd.DataFrame:
    video_raw = act.load_and_prepare(video_input)
    shorts_raw = act.load_and_prepare(shorts_input)

    if category:
        if "category" not in video_raw.columns or "category" not in shorts_raw.columns:
            raise ValueError("Category filtering requested, but input CSVs do not include a 'category' column.")
        video_raw = video_raw.loc[video_raw["category"] == category].copy()
        shorts_raw = shorts_raw.loc[shorts_raw["category"] == category].copy()

    video_agg = act.aggregate_to_timestep(video_raw, step)
    shorts_agg = act.aggregate_to_timestep(shorts_raw, step)

    merged = video_agg[["timestep", metric]].rename(columns={metric: "video"})
    merged = merged.merge(
        shorts_agg[["timestep", metric]].rename(columns={metric: "shorts"}),
        on="timestep",
        how="inner",
    )
    merged = merged.sort_values("timestep").reset_index(drop=True)
    return merged


def stationarity_summary(series: pd.Series) -> dict[str, float | bool | str]:
    cleaned = series.dropna().astype(float)
    if len(cleaned) < 20:
        return {
            "n_obs": int(len(cleaned)),
            "adf_pvalue": np.nan,
            "kpss_pvalue": np.nan,
            "passes_stationarity": False,
            "note": "too_few_observations",
        }

    try:
        adf_pvalue = float(adfuller(cleaned, autolag="AIC")[1])
    except Exception:
        adf_pvalue = np.nan

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InterpolationWarning)
            kpss_pvalue = float(kpss(cleaned, regression="c", nlags="auto")[1])
    except Exception:
        kpss_pvalue = np.nan

    passes = bool(
        pd.notna(adf_pvalue)
        and pd.notna(kpss_pvalue)
        and adf_pvalue < 0.05
        and kpss_pvalue > 0.05
    )
    return {
        "n_obs": int(len(cleaned)),
        "adf_pvalue": adf_pvalue,
        "kpss_pvalue": kpss_pvalue,
        "passes_stationarity": passes,
        "note": "",
    }


def apply_transform(df: pd.DataFrame, transform: str) -> pd.DataFrame:
    transformed = df.copy()
    transformed[["video", "shorts"]] = transformed[["video", "shorts"]].apply(
        pd.to_numeric,
        errors="coerce",
    )
    if transform == "raw":
        return transformed
    if transform == "diff":
        transformed[["video", "shorts"]] = transformed[["video", "shorts"]].diff()
        return transformed.dropna().reset_index(drop=True)
    if transform == "logdiff":
        transformed[["video", "shorts"]] = np.log1p(transformed[["video", "shorts"]]).diff()
        return transformed.dropna().reset_index(drop=True)
    raise ValueError(f"Unsupported transform: {transform}")


def evaluate_transforms(joint_df: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    chosen_transform = TRANSFORM_ORDER[-1]

    for transform in TRANSFORM_ORDER:
        transformed = apply_transform(joint_df, transform)
        video_stats = stationarity_summary(transformed["video"])
        shorts_stats = stationarity_summary(transformed["shorts"])
        rows.append({"transform": transform, "variable": "video", **video_stats})
        rows.append({"transform": transform, "variable": "shorts", **shorts_stats})

        if video_stats["passes_stationarity"] and shorts_stats["passes_stationarity"]:
            chosen_transform = transform
            break

    report = pd.DataFrame(rows)
    report["selected_transform"] = report["transform"] == chosen_transform
    return chosen_transform, report


def run_pcmci(
    transformed_df: pd.DataFrame,
    step: str,
    tau_max: int,
    pc_alpha: float,
    alpha_level: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    var_names = ["video", "shorts"]
    lag_minutes_per_step = pd.Timedelta(step).total_seconds() / 60.0
    dataframe = pp.DataFrame(transformed_df[var_names].to_numpy(), var_names=var_names)
    pcmci = PCMCI(dataframe=dataframe, cond_ind_test=ParCorr(), verbosity=0)
    results = pcmci.run_pcmci(
        tau_max=tau_max,
        pc_alpha=pc_alpha,
        alpha_level=alpha_level,
        fdr_method="fdr_bh",
    )
    q_matrix = pcmci.get_corrected_pvalues(
        results["p_matrix"],
        tau_max=tau_max,
        fdr_method="fdr_bh",
    )

    rows: list[dict[str, object]] = []
    for source_idx, source in enumerate(var_names):
        for target_idx, target in enumerate(var_names):
            for lag in range(1, tau_max + 1):
                rows.append(
                    {
                        "source": source,
                        "target": target,
                        "lag_steps": lag,
                        "lag_minutes": lag * lag_minutes_per_step,
                        "val": float(results["val_matrix"][source_idx, target_idx, lag]),
                        "p_value": float(results["p_matrix"][source_idx, target_idx, lag]),
                        "q_value": float(q_matrix[source_idx, target_idx, lag]),
                        "significant": bool(q_matrix[source_idx, target_idx, lag] <= alpha_level),
                        "cross_series": source != target,
                    }
                )

    links_df = pd.DataFrame(rows).sort_values(["q_value", "source", "target", "lag_steps"])
    significant_df = links_df.loc[links_df["significant"]].reset_index(drop=True)
    return links_df, significant_df


def plot_series(
    raw_df: pd.DataFrame,
    transformed_df: pd.DataFrame,
    transform_name: str,
    step_label: str,
    out_path: Path,
    metric: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=False)
    raw_ax, transformed_ax = axes

    raw_ax.plot(raw_df["timestep"], raw_df["video"], label="Videos", linewidth=1.6)
    raw_ax.plot(raw_df["timestep"], raw_df["shorts"], label="Shorts", linewidth=1.6)
    metric_label = METRIC_LABELS.get(metric, metric)
    raw_ax.set_title(f"{metric_label} per {step_label} Timestep")
    raw_ax.set_ylabel(metric_label)
    raw_ax.grid(True, alpha=0.25)
    raw_ax.legend()

    transformed_ax.plot(
        transformed_df["timestep"],
        transformed_df["video"],
        label="Videos",
        linewidth=1.4,
    )
    transformed_ax.plot(
        transformed_df["timestep"],
        transformed_df["shorts"],
        label="Shorts",
        linewidth=1.4,
    )
    transformed_ax.set_title(f"PCMCI Input Series After {transform_name} Transform")
    transformed_ax.set_xlabel("Captured Timestep (UTC)")
    transformed_ax.set_ylabel("Transformed Value")
    transformed_ax.grid(True, alpha=0.25)
    transformed_ax.legend()

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate original video and shorts time series, make them stationary, "
            "and run PCMCI to test lagged causal structure."
        )
    )
    parser.add_argument(
        "--video-input",
        type=Path,
        default=Path("video_stats_combined.csv"),
        help="Regular videos CSV path (default: video_stats_combined.csv)",
    )
    parser.add_argument(
        "--shorts-input",
        type=Path,
        default=Path("shorts_stats_combined.csv"),
        help="Shorts CSV path (default: shorts_stats_combined.csv)",
    )
    parser.add_argument(
        "--step",
        default=DEFAULT_STEP,
        help=f"Aggregation timestep (default: {DEFAULT_STEP})",
    )
    parser.add_argument(
        "--category",
        default="",
        help="Optional category filter to apply before aggregation (for example: formula1)",
    )
    parser.add_argument(
        "--metric",
        default=DEFAULT_METRIC,
        choices=[
            "total_view_count",
            "new_view_count",
            "new_comment_count",
            "avg_view_count_per_item",
            "avg_new_view_count_per_item",
            "avg_new_comment_count_per_item",
        ],
        help=f"Aggregate metric to analyze (default: {DEFAULT_METRIC})",
    )
    parser.add_argument(
        "--tau-max",
        type=int,
        default=DEFAULT_TAU_MAX,
        help=f"Maximum lag in timesteps for PCMCI (default: {DEFAULT_TAU_MAX})",
    )
    parser.add_argument(
        "--pc-alpha",
        type=float,
        default=DEFAULT_PC_ALPHA,
        help=f"PC stage alpha level (default: {DEFAULT_PC_ALPHA})",
    )
    parser.add_argument(
        "--alpha-level",
        type=float,
        default=DEFAULT_ALPHA_LEVEL,
        help=f"Significance threshold for corrected p-values (default: {DEFAULT_ALPHA_LEVEL})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("final_causality"),
        help="Directory to write PCMCI outputs (default: final_causality)",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{args.metric}_{args.step}"
    if args.category:
        suffix = f"{args.category}_{suffix}"

    joint_df = build_joint_series(args.video_input, args.shorts_input, args.step, args.metric, args.category)
    chosen_transform, stationarity_report = evaluate_transforms(joint_df)
    transformed_df = apply_transform(joint_df, chosen_transform)

    links_df, significant_df = run_pcmci(
        transformed_df=transformed_df,
        step=args.step,
        tau_max=args.tau_max,
        pc_alpha=args.pc_alpha,
        alpha_level=args.alpha_level,
    )

    stationarity_path = args.output_dir / f"stationarity_report_{suffix}.csv"
    transformed_path = args.output_dir / f"pcmci_input_{suffix}.csv"
    links_path = args.output_dir / f"pcmci_links_{suffix}.csv"
    significant_path = args.output_dir / f"pcmci_significant_links_{suffix}.csv"
    summary_path = args.output_dir / f"pcmci_summary_{suffix}.txt"
    plot_path = args.output_dir / f"pcmci_series_{suffix}.png"

    stationarity_report.to_csv(stationarity_path, index=False)
    transformed_df.to_csv(transformed_path, index=False)
    links_df.to_csv(links_path, index=False)
    significant_df.to_csv(significant_path, index=False)
    plot_series(
        raw_df=joint_df,
        transformed_df=transformed_df,
        transform_name=chosen_transform,
        step_label=act.format_step_label(args.step),
        out_path=plot_path,
        metric=args.metric,
    )

    cross_links = significant_df.loc[significant_df["cross_series"]].copy()
    with summary_path.open("w", encoding="utf-8") as fh:
        fh.write(f"metric={args.metric}\n")
        fh.write(f"step={args.step}\n")
        fh.write(f"category={args.category or 'all'}\n")
        fh.write(f"transform={chosen_transform}\n")
        fh.write(f"tau_max={args.tau_max}\n")
        fh.write(f"pc_alpha={args.pc_alpha}\n")
        fh.write(f"alpha_level={args.alpha_level}\n")
        fh.write(f"n_observations={len(transformed_df)}\n")
        fh.write(f"significant_links={len(significant_df)}\n")
        fh.write(f"significant_cross_links={len(cross_links)}\n")

    print(f"Selected transform: {chosen_transform}")
    print(f"Wrote: {stationarity_path.resolve()}")
    print(f"Wrote: {transformed_path.resolve()}")
    print(f"Wrote: {links_path.resolve()}")
    print(f"Wrote: {significant_path.resolve()}")
    print(f"Wrote: {summary_path.resolve()}")
    print(f"Wrote: {plot_path.resolve()}")
    if cross_links.empty:
        print("No significant cross-series links found at the chosen alpha level.")
    else:
        print("Significant cross-series links:")
        for _, row in cross_links.iterrows():
            print(
                f"  {row['source']} -> {row['target']} "
                f"(lag={int(row['lag_steps'])} steps / {row['lag_minutes']:.0f} min, val={row['val']:.4f}, "
                f"q={row['q_value']:.4g})"
            )


if __name__ == "__main__":
    main()
