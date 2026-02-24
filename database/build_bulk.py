import csv
import json
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any
from src.utils.logger import get_logger


logger = get_logger(__name__)


def write_tsv(path: Path, header: list[str], rows) -> None:
    with path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(header)
        writer.writerows(rows)


def build_bulk_from_rows(rows: list[dict[str, Any]], out_dir: Path) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)

    items = set()
    sessions = set()
    next_agg = defaultdict(int)
    contains = set()

    for row in rows:
        item_id = row.get("item_id")
        next_item_id = row.get("next_item_id")
        session_id = row.get("session_id")

        if item_id is None:
            continue

        item_id = int(item_id)
        items.add(item_id)

        if session_id is not None:
            session_id = int(session_id)
            sessions.add(session_id)
            contains.add((session_id, item_id))

        if next_item_id is not None:
            next_item_id = int(next_item_id)
            items.add(next_item_id)
            next_agg[(item_id, next_item_id)] += 1

    items = sorted(items)
    sessions = sorted(sessions)
    next_rows = sorted(next_agg.items())
    contains_rows = sorted(contains)

    cooccurs_agg = defaultdict(int)
    current_session = None
    session_items: list[int] = []

    for session_id, item_id in contains_rows:
        if current_session is None:
            current_session = session_id

        if session_id != current_session:
            if len(session_items) >= 2:
                for i1, i2 in combinations(session_items, 2):
                    cooccurs_agg[(i1, i2)] += 1
            current_session = session_id
            session_items = []

        session_items.append(item_id)

    if len(session_items) >= 2:
        for i1, i2 in combinations(session_items, 2):
            cooccurs_agg[(i1, i2)] += 1

    cooccurs_rows = sorted((src, dst, w) for (src, dst), w in cooccurs_agg.items())

    write_tsv(out_dir / "items.csv", ["item_id"], ((item_id,) for item_id in items))
    write_tsv(out_dir / "sessions.csv", ["session_id"], ((session_id,) for session_id in sessions))
    write_tsv(
        out_dir / "next_typed.csv",
        ["src_item_id", "dst_item_id", "weight"],
        ((src, dst, w) for (src, dst), w in next_rows),
    )
    write_tsv(out_dir / "contains_typed.csv", ["session_id", "item_id"], contains_rows)
    write_tsv(
        out_dir / "cooccurs_typed.csv",
        ["src_item_id", "dst_item_id", "weight"],
        cooccurs_rows,
    )

    lookup = {
        "item_to_id": {str(item_id): item_id for item_id in items},
        "session_to_id": {str(session_id): session_id for session_id in sessions},
    }
    with (out_dir / "lookup.json").open("w") as f:
        json.dump(lookup, f)

    return {
        "items": len(items),
        "sessions": len(sessions),
        "next_edges": len(next_rows),
        "contains_edges": len(contains_rows),
        "cooccurs_edges": len(cooccurs_rows),
    }


def build_bulk_from_json(data_path: Path, out_dir: Path) -> dict[str, int]:
    logger.info("Loading batch rows from %s", data_path)
    rows = json.load(data_path.open())
    stats = build_bulk_from_rows(rows, out_dir)
    logger.info("Items: %s", f"{stats['items']:,}")
    logger.info("Sessions: %s", f"{stats['sessions']:,}")
    logger.info("NEXT edges: %s", f"{stats['next_edges']:,}")
    logger.info("CONTAINS edges: %s", f"{stats['contains_edges']:,}")
    logger.info("CO_OCCURS edges: %s", f"{stats['cooccurs_edges']:,}")
    logger.info("Bulk files ready in %s", out_dir)
    return stats


def main() -> None:
    data_path = Path("data/batch.json")
    out_dir = Path("data/neo4j_import")
    build_bulk_from_json(data_path, out_dir)


if __name__ == "__main__":
    main()
