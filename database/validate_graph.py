import argparse
from pathlib import Path

try:
    from database.driver import Neo4jDriver
except ModuleNotFoundError:
    from driver import Neo4jDriver


def tsv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as f:
        # subtract header line
        return max(sum(1 for _ in f) - 1, 0)


def fail(message: str) -> None:
    raise SystemExit(f"VALIDATION FAILED: {message}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Neo4j graph invariants")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/neo4j_import"),
        help="Directory containing generated import files",
    )
    args = parser.parse_args()

    source = args.source_dir
    expected = {
        "items": tsv_rows(source / "items.csv"),
        "sessions": tsv_rows(source / "sessions.csv"),
        "contains": tsv_rows(source / "contains_typed.csv"),
        "nexts": tsv_rows(source / "next_typed.csv"),
        "cooccurs": tsv_rows(source / "cooccurs_typed.csv"),
    }

    driver = Neo4jDriver()
    try:
        result = driver.read_query(
            """
            MATCH (i:Item)
            WITH count(i) AS items
            MATCH (s:Session)
            WITH items, count(s) AS sessions
            MATCH ()-[c:CONTAINS]->()
            WITH items, sessions, count(c) AS contains
            MATCH ()-[n:NEXT]->()
            WITH items, sessions, contains, count(n) AS nexts
            MATCH ()-[co:CO_OCCURS]->()
            RETURN items, sessions, contains, nexts, count(co) AS cooccurs
            """
        )[0]

        quality = driver.read_query(
            """
            OPTIONAL MATCH (s:Session) WHERE s.session_id IS NULL
            WITH count(s) AS null_sessions
            OPTIONAL MATCH (i:Item) WHERE i.item_id IS NULL
            WITH null_sessions, count(i) AS null_items
            OPTIONAL MATCH ()-[n:NEXT]->() WHERE n.weight IS NULL OR n.weight <= 0
            WITH null_sessions, null_items, count(n) AS bad_next_weights
            OPTIONAL MATCH ()-[c:CO_OCCURS]->() WHERE c.weight IS NULL OR c.weight <= 0
            RETURN null_sessions, null_items, bad_next_weights, count(c) AS bad_co_weights
            """
        )[0]

    finally:
        driver.close()

    print("Expected counts:", expected)
    print("Graph counts:", result)
    print("Quality checks:", quality)

    for k, v in expected.items():
        if int(result[k]) != int(v):
            fail(f"count mismatch for {k}: expected={v}, actual={result[k]}")

    if quality["null_sessions"] != 0:
        fail(f"null session_id nodes found: {quality['null_sessions']}")
    if quality["null_items"] != 0:
        fail(f"null item_id nodes found: {quality['null_items']}")
    if quality["bad_next_weights"] != 0:
        fail(f"NEXT edges with invalid weight: {quality['bad_next_weights']}")
    if quality["bad_co_weights"] != 0:
        fail(f"CO_OCCURS edges with invalid weight: {quality['bad_co_weights']}")

    print("Graph validation passed.")


if __name__ == "__main__":
    main()
