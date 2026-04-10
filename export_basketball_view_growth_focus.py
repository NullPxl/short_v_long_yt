from __future__ import annotations

import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import plot_pcmci_lag_window as plw
import run_pcmci_causality as rpc


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "final_causality" / "basketball_view_growth_focus"
CATEGORY = "basketball"
METRIC = "avg_new_view_count_per_item"
STEP = "5min"


def zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if std == 0 or np.isnan(std):
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - float(series.mean())) / std


def strength_label(val: float) -> str:
    magnitude = abs(val)
    if magnitude >= 0.30:
        return "strong"
    if magnitude >= 0.20:
        return "moderate"
    if magnitude >= 0.10:
        return "modest"
    if magnitude >= 0.05:
        return "weak"
    return "very weak"


def build_directional_lag_profile(
    transformed_df: pd.DataFrame,
    source: str,
    target: str,
    step: str,
    min_lag_minutes: float,
    max_lag_minutes: float,
) -> pd.DataFrame:
    step_minutes = pd.Timedelta(step).total_seconds() / 60.0
    min_lag_steps = max(1, int(math.ceil(min_lag_minutes / step_minutes)))
    max_lag_steps = max(min_lag_steps, int(math.floor(max_lag_minutes / step_minutes)))

    rows: list[dict[str, float]] = []
    for lag_steps in range(min_lag_steps, max_lag_steps + 1):
        aligned = pd.DataFrame(
            {
                "target": transformed_df[target],
                "source_shifted": transformed_df[source].shift(lag_steps),
            }
        ).dropna()
        rows.append(
            {
                "lag_steps": lag_steps,
                "lag_minutes": lag_steps * step_minutes,
                "correlation": float(aligned["target"].corr(aligned["source_shifted"])),
                "n_obs": float(len(aligned)),
            }
        )
    return pd.DataFrame(rows)


def build_aligned_frame(df: pd.DataFrame, source: str, target: str, lag_steps: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestep": df["timestep"],
            "target": df[target],
            "source_shifted": df[source].shift(lag_steps),
        }
    ).dropna().reset_index(drop=True)


def plot_cross_link(
    raw_df: pd.DataFrame,
    transformed_df: pd.DataFrame,
    all_cross_links: pd.DataFrame,
    row: pd.Series,
    out_path: Path,
) -> None:
    source = str(row["source"])
    target = str(row["target"])
    lag_steps = int(row["lag_steps"])
    lag_minutes = float(row["lag_minutes"])
    val = float(row["val"])
    q_value = float(row["q_value"])

    lag_profile = build_directional_lag_profile(
        transformed_df=transformed_df,
        source=source,
        target=target,
        step=STEP,
        min_lag_minutes=5.0,
        max_lag_minutes=60.0,
    )
    direction_links = all_cross_links.loc[
        (all_cross_links["source"] == source) & (all_cross_links["target"] == target)
    ].copy()

    raw_aligned = build_aligned_frame(raw_df, source, target, lag_steps)
    transformed_aligned = build_aligned_frame(transformed_df, source, target, lag_steps)

    target_raw = zscore(raw_aligned["target"])
    source_raw = zscore(raw_aligned["source_shifted"])
    target_transformed = zscore(transformed_aligned["target"])
    source_transformed = zscore(transformed_aligned["source_shifted"])

    fig, axes = plt.subplots(3, 1, figsize=(13, 12))
    lag_ax, raw_ax, transformed_ax = axes

    lag_ax.plot(
        lag_profile["lag_minutes"],
        lag_profile["correlation"],
        linewidth=2.0,
        color="#1f77b4",
    )
    for _, link in direction_links.iterrows():
        link_lag = float(link["lag_minutes"])
        corr = lag_profile.loc[lag_profile["lag_steps"] == int(link["lag_steps"]), "correlation"].iloc[0]
        color = "#d62728" if int(link["lag_steps"]) == lag_steps else "#ff9896"
        size = 80 if int(link["lag_steps"]) == lag_steps else 45
        lag_ax.scatter(link_lag, corr, color=color, s=size, zorder=3)
    lag_ax.axvline(lag_minutes, color="#d62728", linestyle="--", linewidth=1.5)
    lag_ax.set_xlim(5, 60)
    lag_ax.set_title(f"Lag Profile: {source.capitalize()} Leading {target.capitalize()}")
    lag_ax.set_xlabel("Lag Minutes")
    lag_ax.set_ylabel("Pearson Correlation")
    lag_ax.grid(True, alpha=0.25)

    raw_ax.plot(
        raw_aligned["timestep"],
        target_raw,
        linewidth=1.4,
        label=f"{target.capitalize()} current",
    )
    raw_ax.plot(
        raw_aligned["timestep"],
        source_raw,
        linewidth=1.4,
        label=f"{source.capitalize()} shifted by {lag_minutes:.0f} min",
    )
    raw_ax.scatter(raw_aligned["timestep"], target_raw, s=10, alpha=0.35, color="#1f77b4")
    raw_ax.scatter(raw_aligned["timestep"], source_raw, s=10, alpha=0.35, color="#ff7f0e")
    raw_ax.set_title("Raw Aligned Series (z-scored)")
    raw_ax.set_ylabel("Z-Score")
    raw_ax.grid(True, alpha=0.25)
    raw_ax.legend()

    transformed_ax.plot(
        transformed_aligned["timestep"],
        target_transformed,
        linewidth=1.4,
        label=f"{target.capitalize()} PCMCI input",
    )
    transformed_ax.plot(
        transformed_aligned["timestep"],
        source_transformed,
        linewidth=1.4,
        label=f"{source.capitalize()} shifted by {lag_minutes:.0f} min",
    )
    transformed_ax.scatter(transformed_aligned["timestep"], target_transformed, s=10, alpha=0.35, color="#1f77b4")
    transformed_ax.scatter(transformed_aligned["timestep"], source_transformed, s=10, alpha=0.35, color="#ff7f0e")
    transformed_ax.set_title(
        f"Transformed Alignment for PCMCI Link (val={val:.3f}, q={q_value:.3g}, {strength_label(val)})"
    )
    transformed_ax.set_xlabel("Captured Timestep (UTC)")
    transformed_ax.set_ylabel("Z-Score")
    transformed_ax.grid(True, alpha=0.25)
    transformed_ax.legend()

    fig.autofmt_xdate()
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw_df = rpc.build_joint_series(
        ROOT / "video_stats_combined.csv",
        ROOT / "shorts_stats_combined.csv",
        STEP,
        METRIC,
        CATEGORY,
    )
    transformed_df = pd.read_csv(
        ROOT / "final_causality" / "basketball" / "pcmci_input_basketball_avg_new_view_count_per_item_5min.csv",
        parse_dates=["timestep"],
    )
    links_df = pd.read_csv(
        ROOT / "final_causality" / "basketball" / "pcmci_links_basketball_avg_new_view_count_per_item_5min.csv"
    )
    summary_txt = (ROOT / "final_causality" / "basketball" / "pcmci_summary_basketball_avg_new_view_count_per_item_5min.txt").read_text(encoding="utf-8")
    stationarity_df = pd.read_csv(
        ROOT / "final_causality" / "basketball" / "stationarity_report_basketball_avg_new_view_count_per_item_5min.csv"
    )

    cross_links = links_df.loc[links_df["cross_series"] & links_df["significant"]].copy()
    cross_links = cross_links.sort_values("q_value").reset_index(drop=True)
    cross_links.to_csv(OUTPUT_DIR / "basketball_view_growth_significant_cross_links.csv", index=False)
    stationarity_df.to_csv(OUTPUT_DIR / "basketball_view_growth_stationarity.csv", index=False)
    transformed_df.to_csv(OUTPUT_DIR / "basketball_view_growth_pcmci_input.csv", index=False)
    (OUTPUT_DIR / "basketball_view_growth_summary.txt").write_text(summary_txt, encoding="utf-8")

    report_lines = [
        "# Basketball View Growth PCMCI Focus",
        "",
        "These files isolate the normalized basketball view-growth PCMCI analysis only.",
        "",
        "Interpretation guide:",
        "- `lag_minutes`: how far the source series leads the target series in real UTC time.",
        "- `val`: the PCMCI partial-correlation estimate on the transformed (`diff`) series.",
        "- Positive `val` means higher source growth tends to be followed by higher target growth at that lag.",
        "- Negative `val` means higher source growth tends to be followed by lower target growth at that lag.",
        "- Strength labels here are rough effect-size descriptors for `|val|`: very weak < 0.05, weak 0.05-0.10, modest 0.10-0.20, moderate 0.20-0.30, strong >= 0.30.",
        "",
        "Selected transform:",
    ]
    selected_rows = stationarity_df.loc[stationarity_df["selected_transform"]]
    if not selected_rows.empty:
        report_lines.append(f"- `{selected_rows['transform'].iloc[0]}`")
    report_lines.extend(["", "Significant cross-links:"])

    for idx, (_, row) in enumerate(cross_links.iterrows(), start=1):
        source = str(row["source"])
        target = str(row["target"])
        lag_minutes = float(row["lag_minutes"])
        val = float(row["val"])
        q_value = float(row["q_value"])
        sign_label = "positive" if val > 0 else "negative"
        filename = (
            f"{idx:02d}_{source}_to_{target}_{int(lag_minutes)}min.png"
        )
        plot_cross_link(
            raw_df=raw_df,
            transformed_df=transformed_df,
            all_cross_links=cross_links,
            row=row,
            out_path=OUTPUT_DIR / filename,
        )
        report_lines.extend(
            [
                f"## {idx}. {source.capitalize()} -> {target.capitalize()} at {lag_minutes:.0f} min",
                f"- Plot: `{filename}`",
                f"- PCMCI val: `{val:.4f}` ({strength_label(val)}, {sign_label})",
                f"- q-value: `{q_value:.3g}`",
                (
                    f"- Reading: a stronger {source} view-growth signal is associated with "
                    f"{'higher' if val > 0 else 'lower'} {target} view growth about {lag_minutes:.0f} minutes later, "
                    f"after conditioning on the two-series lag structure."
                ),
                "",
            ]
        )

    (OUTPUT_DIR / "README.md").write_text("\n".join(report_lines), encoding="utf-8")
    print(f"Wrote focus package to: {OUTPUT_DIR}")
    print(f"Cross-links exported: {len(cross_links)}")


if __name__ == "__main__":
    main()
