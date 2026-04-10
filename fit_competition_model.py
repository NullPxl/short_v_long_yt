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
DEFAULT_STATE_COLUMN = "avg_new_view_count_per_item"
DEFAULT_TARGET_MODE = "delta"


def build_aligned_states(
    video_input: Path,
    shorts_input: Path,
    step: str,
    state_column: str,
    target_mode: str,
    category: str,
) -> pd.DataFrame:
    video_raw = act.load_and_prepare(video_input)
    shorts_raw = act.load_and_prepare(shorts_input)

    if category:
        if "category" not in video_raw.columns or "category" not in shorts_raw.columns:
            raise ValueError("Category filtering requested, but input CSVs do not include a 'category' column.")
        video_raw = video_raw.loc[video_raw["category"] == category].copy()
        shorts_raw = shorts_raw.loc[shorts_raw["category"] == category].copy()

    video = act.aggregate_to_timestep(video_raw, step)
    shorts = act.aggregate_to_timestep(shorts_raw, step)

    merged = shorts[["timestep", state_column]].rename(columns={state_column: "S"})
    merged = merged.merge(
        video[["timestep", state_column]].rename(columns={state_column: "V"}),
        on="timestep",
        how="inner",
    )
    merged = merged.sort_values("timestep").reset_index(drop=True)
    if target_mode == "delta":
        merged["target_S"] = merged["S"].diff()
        merged["target_V"] = merged["V"].diff()
        merged = merged.dropna().reset_index(drop=True)
    elif target_mode == "next_state":
        merged["target_S"] = merged["S"].shift(-1)
        merged["target_V"] = merged["V"].shift(-1)
        merged = merged.dropna().reset_index(drop=True)
    else:
        raise ValueError(f"Unsupported target mode: {target_mode}")
    return merged


def fit_side(state: np.ndarray, other: np.ndarray, delta: np.ndarray) -> tuple[np.ndarray, np.ndarray, float]:
    state = state.astype(float)
    other = other.astype(float)
    delta = delta.astype(float)
    X = np.column_stack([state, state**2, state * other, np.ones_like(state)])
    coef = np.linalg.lstsq(X, delta, rcond=None)[0]
    pred = X @ coef
    denom = np.sum((delta - np.mean(delta)) ** 2)
    r2 = 1.0 - np.sum((delta - pred) ** 2) / denom if denom > 0 else float("nan")
    return coef, pred, float(r2)


def plot_fit(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax_s, ax_v = axes

    ax_s.plot(df["timestep"], df["target_S"], label="Actual shorts target", linewidth=1.5)
    ax_s.plot(df["timestep"], df["pred_target_S"], label="Predicted shorts target", linewidth=1.5)
    ax_s.set_title("Competition Model Fit for Shorts")
    ax_s.set_ylabel("Target per Timestep")
    ax_s.grid(True, alpha=0.25)
    ax_s.legend()

    ax_v.plot(df["timestep"], df["target_V"], label="Actual videos target", linewidth=1.5)
    ax_v.plot(df["timestep"], df["pred_target_V"], label="Predicted videos target", linewidth=1.5)
    ax_v.set_title("Competition Model Fit for Videos")
    ax_v.set_xlabel("Captured Timestep (UTC)")
    ax_v.set_ylabel("Target per Timestep")
    ax_v.grid(True, alpha=0.25)
    ax_v.legend()

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fit a two-format competition model of the form "
            "dS/dt = aS*S - bS*S^2 - cS*S*V and "
            "dV/dt = aV*V - bV*V^2 - cV*S*V."
        )
    )
    parser.add_argument("--video-input", type=Path, default=Path("video_stats_combined.csv"))
    parser.add_argument("--shorts-input", type=Path, default=Path("shorts_stats_combined.csv"))
    parser.add_argument("--step", default="5min")
    parser.add_argument("--category", default="", help="Optional category filter, e.g. basketball")
    parser.add_argument(
        "--state-column",
        default=DEFAULT_STATE_COLUMN,
        choices=[
            "new_view_count",
            "total_view_count",
            "avg_new_view_count_per_item",
            "avg_view_count_per_item",
        ],
        help=f"State variable to model (default: {DEFAULT_STATE_COLUMN})",
    )
    parser.add_argument(
        "--target-mode",
        default=DEFAULT_TARGET_MODE,
        choices=["next_state", "delta"],
        help="Whether to fit next-timestep state directly or timestep-to-timestep change (default: delta)",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("final_competition"))
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    df = build_aligned_states(
        args.video_input,
        args.shorts_input,
        args.step,
        args.state_column,
        args.target_mode,
        args.category,
    )
    numeric_columns = ["S", "V", "target_S", "target_V"]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=numeric_columns).reset_index(drop=True)

    scale = max(
        float(df["S"].abs().max()),
        float(df["V"].abs().max()),
        float(df["target_S"].abs().max()),
        float(df["target_V"].abs().max()),
        1.0,
    )
    df["S_scaled"] = df["S"] / scale
    df["V_scaled"] = df["V"] / scale
    df["target_S_scaled"] = df["target_S"] / scale
    df["target_V_scaled"] = df["target_V"] / scale

    coef_S, pred_target_S_scaled, r2_S = fit_side(
        state=df["S_scaled"].to_numpy(),
        other=df["V_scaled"].to_numpy(),
        delta=df["target_S_scaled"].to_numpy(),
    )
    coef_V, pred_target_V_scaled, r2_V = fit_side(
        state=df["V_scaled"].to_numpy(),
        other=df["S_scaled"].to_numpy(),
        delta=df["target_V_scaled"].to_numpy(),
    )

    beta1_S, beta2_S, beta3_S, intercept_S_scaled = coef_S
    beta1_V, beta2_V, beta3_V, intercept_V_scaled = coef_V

    a_S = float(beta1_S)
    b_S = float(-beta2_S / scale)
    c_S = float(-beta3_S / scale)
    intercept_S = float(intercept_S_scaled * scale)

    a_V = float(beta1_V)
    b_V = float(-beta2_V / scale)
    c_V = float(-beta3_V / scale)
    intercept_V = float(intercept_V_scaled * scale)

    alpha = float(c_S / b_S) if b_S != 0 else float("nan")
    beta = float(c_V / b_V) if b_V != 0 else float("nan")

    df["pred_target_S"] = pred_target_S_scaled * scale
    df["pred_target_V"] = pred_target_V_scaled * scale

    coefficients = pd.DataFrame(
        [
            {
                "side": "shorts",
                "a": a_S,
                "b": b_S,
                "c": c_S,
                "intercept": intercept_S,
                "r2": r2_S,
            },
            {
                "side": "videos",
                "a": a_V,
                "b": b_V,
                "c": c_V,
                "intercept": intercept_V,
                "r2": r2_V,
            },
        ]
    )
    ratios = pd.DataFrame([{"alpha": alpha, "beta": beta, "scale_used": scale}])

    suffix = f"{args.state_column}_{args.target_mode}_{args.step}"
    if args.category:
        suffix = f"{args.category}_{suffix}"
    coefficients_path = args.output_dir / f"competition_coefficients_{suffix}.csv"
    ratios_path = args.output_dir / f"competition_ratios_{suffix}.csv"
    aligned_path = args.output_dir / f"competition_series_{suffix}.csv"
    plot_path = args.output_dir / f"competition_fit_{suffix}.png"

    coefficients.to_csv(coefficients_path, index=False)
    ratios.to_csv(ratios_path, index=False)
    df[["timestep", "S", "V", "target_S", "target_V", "pred_target_S", "pred_target_V"]].to_csv(
        aligned_path,
        index=False,
    )
    plot_fit(df, plot_path)

    print(f"Wrote: {coefficients_path.resolve()}")
    print(f"Wrote: {ratios_path.resolve()}")
    print(f"Wrote: {aligned_path.resolve()}")
    print(f"Wrote: {plot_path.resolve()}")
    print(f"category={args.category or 'all'}")
    print(f"target_mode={args.target_mode}")
    print(f"a_S={a_S:.6f}, b_S={b_S:.6f}, c_S={c_S:.6f}, r2_S={r2_S:.4f}")
    print(f"a_V={a_V:.6f}, b_V={b_V:.6f}, c_V={c_V:.6f}, r2_V={r2_V:.4f}")
    print(f"alpha={alpha:.6f}, beta={beta:.6f}, scale_used={scale:.6f}")


if __name__ == "__main__":
    main()
