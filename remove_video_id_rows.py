from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def remove_video_id_rows(csv_path: Path, video_id: str) -> int:
    df = pd.read_csv(csv_path)
    if "video_id" not in df.columns:
        raise ValueError(f"{csv_path} is missing required column: video_id")

    before = len(df)
    df = df[df["video_id"] != video_id]
    removed = before - len(df)
    df.to_csv(csv_path, index=False)
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove rows with a specific video_id from both video_stats.csv and shorts_stats.csv."
    )
    parser.add_argument("--video-id", default="8AQJkdIxf6k", help="video_id to remove")
    parser.add_argument("--video-csv", type=Path, default=Path("video_stats.csv"))
    parser.add_argument("--shorts-csv", type=Path, default=Path("shorts_stats.csv"))
    args = parser.parse_args()

    removed_video = remove_video_id_rows(args.video_csv, args.video_id)
    removed_shorts = remove_video_id_rows(args.shorts_csv, args.video_id)

    print(f"Removed {removed_video} rows from {args.video_csv}")
    print(f"Removed {removed_shorts} rows from {args.shorts_csv}")


if __name__ == "__main__":
    main()
