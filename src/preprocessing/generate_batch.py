import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def build_batch_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    required = {"session_id", "item_id"}
    if not required.issubset(df.columns):
        raise ValueError("Input parquet must contain session_id and item_id columns")

    sort_cols = ["session_id"]
    if "position" in df.columns:
        sort_cols.append("position")
    elif "timestamp" in df.columns:
        sort_cols.append("timestamp")

    rows: list[dict[str, Any]] = []
    ordered = df.sort_values(sort_cols)
    for session_id, group in ordered.groupby("session_id", sort=True):
        items = [int(x) for x in group["item_id"].tolist()]
        for idx, item_id in enumerate(items):
            next_item = items[idx + 1] if (idx + 1) < len(items) else None
            rows.append(
                {
                    "session_id": int(session_id),
                    "item_id": int(item_id),
                    "next_item_id": int(next_item) if next_item is not None else None,
                }
            )
    return rows


def generate_batch_json(input_parquet: Path, output_json: Path) -> int:
    if not input_parquet.exists():
        raise FileNotFoundError(f"Input not found: {input_parquet}")

    df = pd.read_parquet(input_parquet)
    rows = build_batch_rows(df)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    with output_json.open("w") as f:
        json.dump(rows, f, indent=2)

    logger.info("Batch JSON generated: %s rows -> %s", f"{len(rows):,}", output_json)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate batch.json from processed parquet sessions")
    parser.add_argument("--input", type=Path, default=Path("data/processed/yoochoose_sessions.parquet"))
    parser.add_argument("--output", type=Path, default=Path("data/batch.json"))
    args = parser.parse_args()

    generate_batch_json(args.input, args.output)


if __name__ == "__main__":
    main()
