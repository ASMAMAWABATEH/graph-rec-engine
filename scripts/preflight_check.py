"""Verify Neo4j connectivity and required environment variables."""

import os

from dotenv import load_dotenv
from neo4j import GraphDatabase


def main() -> int:
    load_dotenv()

    required = ("NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        return 1

    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        driver.verify_connectivity()
        with driver.session(database=database) as session:
            ok = session.run("RETURN 1 AS ok").single()["ok"] == 1
        print(f"Neo4j reachable: {ok}")
        return 0 if ok else 1
    except Exception as exc:
        print(f"Neo4j preflight failed: {exc}")
        return 1
    finally:
        driver.close()


if __name__ == "__main__":
    raise SystemExit(main())
