from __future__ import annotations

from pathlib import Path

import pandas as pd


SHORTS_INPUTS = [
    (Path("shorts_stats.csv"), "basketball"),
    (Path("shorts_stats_marchmadness.csv"), "basketball"),
    (Path("shorts_stats_formula1.csv"), "formula1"),
    (Path("gothamchess_shorts_stats.csv"), "chess"),
]

VIDEO_INPUTS = [
    (Path("video_stats.csv"), "basketball"),
    (Path("videos_stats_marchmadness.csv"), "basketball"),
    (Path("videos_stats_formula1.csv"), "formula1"),
    (Path("gothamchess_video_stats.csv"), "chess"),
]


def combine_csvs(inputs: list[tuple[Path, str]], output_path: Path) -> None:
    frames: list[pd.DataFrame] = []
    for csv_path, category in inputs:
        df = pd.read_csv(csv_path)
        df["source_dataset"] = csv_path.stem
        df["category"] = category
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    sort_columns = [column for column in ("captured_at", "video_id", "source_dataset") if column in combined.columns]
    if sort_columns:
        combined = combined.sort_values(sort_columns, kind="stable").reset_index(drop=True)

    combined.to_csv(output_path, index=False)


def main() -> None:
    combine_csvs(SHORTS_INPUTS, Path("shorts_stats_combined.csv"))
    combine_csvs(VIDEO_INPUTS, Path("video_stats_combined.csv"))
    print(f"Wrote: {Path('shorts_stats_combined.csv').resolve()}")
    print(f"Wrote: {Path('video_stats_combined.csv').resolve()}")


if __name__ == "__main__":
    main()
