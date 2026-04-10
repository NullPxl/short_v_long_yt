from __future__ import annotations

import subprocess
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PYTHON = SCRIPT_DIR / "venv" / "Scripts" / "python.exe"
OUTPUT_DIR = SCRIPT_DIR / "final_causality"
STEP = "5min"
METRICS = [
    "avg_view_count_per_item",
    "avg_new_view_count_per_item",
    "avg_new_comment_count_per_item",
]


def run_command(args: list[str]) -> None:
    subprocess.run(args, cwd=SCRIPT_DIR, check=True)


def main() -> None:
    run_command([str(PYTHON), "combine_competition_csvs.py"])
    for metric in METRICS:
        run_command(
            [
                str(PYTHON),
                "run_pcmci_causality.py",
                "--metric",
                metric,
                "--step",
                STEP,
                "--output-dir",
                str(OUTPUT_DIR),
            ]
        )


if __name__ == "__main__":
    main()
