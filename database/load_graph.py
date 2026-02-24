import argparse
import os
import shutil
import time
from pathlib import Path
from src.utils.logger import get_logger

try:
    from database.driver import Neo4jDriver
except ModuleNotFoundError:
    from driver import Neo4jDriver
from neo4j.exceptions import TransientError


logger = get_logger(__name__)


DEFAULT_CYPHER_FILES = [
    Path("database/cypher/schema.cypher"),
    Path("database/cypher/build.cypher"),
    Path("database/cypher/cooccurs.cypher"),
]

REQUIRED_IMPORT_FILES = [
    "items.csv",
    "sessions.csv",
    "contains_typed.csv",
    "next_typed.csv",
    "cooccurs_typed.csv",
]


def parse_cypher_statements(path: Path) -> list[str]:
    lines: list[str] = []
    for raw in path.read_text().splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("//") or stripped.startswith(":param"):
            continue
        lines.append(raw)
    text = "\n".join(lines)
    return [part.strip() for part in text.split(";") if part.strip()]


def stage_import_files(source_dir: Path, neo4j_import_dir: Path) -> None:
    neo4j_import_dir.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED_IMPORT_FILES:
        src = source_dir / name
        if not src.exists():
            raise FileNotFoundError(f"Required import file missing: {src}")
        dst = neo4j_import_dir / name
        shutil.copy2(src, dst)
        logger.info("Staged %s -> %s", src, dst)


def run_cypher_files(files: list[Path]) -> None:
    driver = Neo4jDriver()
    try:
        database = os.getenv("NEO4J_DATABASE")
        with driver.driver.session(database=database) as session:
            for file_path in files:
                statements = parse_cypher_statements(file_path)
                logger.info("Running %s (%s statements)", file_path, len(statements))
                for i, statement in enumerate(statements, 1):
                    # Implicit transaction is required for CALL ... IN TRANSACTIONS.
                    retries = 0
                    while True:
                        try:
                            session.run(statement).consume()
                            break
                        except TransientError as exc:
                            retries += 1
                            if retries > 5:
                                raise
                            sleep_s = 2 ** (retries - 1)
                            logger.warning(
                                "Transient error on statement %s retry %s/5 in %ss: %s",
                                i,
                                retries,
                                sleep_s,
                                exc.code,
                            )
                            time.sleep(sleep_s)
                    logger.info("  [%s/%s] OK", i, len(statements))
    finally:
        driver.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Graph-SBR data into Neo4j")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/neo4j_import"),
        help="Directory containing generated CSV/TSV files",
    )
    parser.add_argument(
        "--neo4j-import-dir",
        type=Path,
        default=None,
        help="Neo4j import directory to stage files into before LOAD CSV",
    )
    parser.add_argument(
        "--skip-cooccurs",
        action="store_true",
        help="Skip CO_OCCURS loading phase",
    )
    args = parser.parse_args()

    if args.neo4j_import_dir is not None:
        stage_import_files(args.source_dir, args.neo4j_import_dir)
    else:
        logger.info("No --neo4j-import-dir provided; assuming files are already in Neo4j import dir.")

    files = DEFAULT_CYPHER_FILES if not args.skip_cooccurs else DEFAULT_CYPHER_FILES[:-1]
    run_cypher_files(files)
    logger.info("Graph load complete.")


if __name__ == "__main__":
    main()
