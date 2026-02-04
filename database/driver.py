import os
import json
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging
import argparse

# ---------------------------
# Load .env variables
# ---------------------------
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------
# Neo4j Driver Wrapper
# ---------------------------
class Neo4jDriver:
    def __init__(self):
        """
        Initialize the Neo4j driver using environment variables.
        Verifies connectivity (Step 0.1 invariant).
        """
        if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
            raise ValueError("Neo4j connection variables missing in .env")

        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

        # Pre-flight connectivity check
        self.driver.verify_connectivity()
        logger.info(f"🚀 Connected to Neo4j at {NEO4J_URI} (DB: {NEO4J_DATABASE})")

    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()
        logger.info("Neo4j connection closed.")

    # ---------------------------
    # READ-ONLY queries
    # ---------------------------
    def read_query(self, query: str, params: dict = None):
        """Execute a strictly read-only query."""
        def _read_tx(tx):
            result = tx.run(query, params or {})
            return [record.data() for record in result]

        with self.driver.session(database=NEO4J_DATABASE) as session:
            return session.execute_read(_read_tx)

    # ---------------------------
    # WRITE queries
    # ---------------------------
    def write_query(self, query: str, params: dict = None):
        """Execute a write query with automatic transaction handling."""
        def _write_tx(tx):
            result = tx.run(query, params or {})
            return result.consume()

        with self.driver.session(database=NEO4J_DATABASE) as session:
            return session.execute_write(_write_tx)

    # ---------------------------
    # BATCH WRITE (optimized for $batch UNWIND) - FIXED VERSION
    # ---------------------------
     # ---------------------------
    # BATCH WRITE – SAFE STREAMING VERSION
    # ---------------------------
    def execute_batch(self, query: str, batch: list):
        """
        Memory-safe UNWIND loader with:
        - chunk streaming
        - retry on bolt timeout
        - aggregated counters
        """
        from neo4j.exceptions import ServiceUnavailable, SessionExpired
        import time

        CHUNK_SIZE = 10000
        MAX_RETRIES = 3

        total = len(batch)
        logger.info(f"🚀 BATCH START: {total} rows → DB: {NEO4J_DATABASE}")
        logger.info(f"📋 Query preview: {query[:150]}...")

        overall = {
            "nodes": 0,
            "rels": 0,
            "props": 0
        }

        def _batch_tx(tx, chunk):
            result = tx.run(query, {"batch": chunk})
            return result.consume()

        try:
            with self.driver.session(database=NEO4J_DATABASE) as session:

                for i in range(0, total, CHUNK_SIZE):
                    chunk = batch[i:i + CHUNK_SIZE]

                    for attempt in range(MAX_RETRIES):
                        try:
                            summary = session.execute_write(_batch_tx, chunk)

                            overall["nodes"] += summary.counters.nodes_created
                            overall["rels"] += summary.counters.relationships_created
                            overall["props"] += summary.counters.properties_set

                            logger.info(
                                f"   ✅ {min(i+CHUNK_SIZE, total)}/{total} "
                                f"(+{summary.counters.nodes_created} nodes, "
                                f"+{summary.counters.relationships_created} rels)"
                            )

                            # log any cypher notifications
                            if summary.notifications:
                                for n in summary.notifications:
                                    logger.warning(f"   {n.title}: {n.description}")

                            break

                        except (ServiceUnavailable, SessionExpired) as e:
                            wait = 2 ** attempt
                            logger.warning(
                                f"⚠️ Chunk failed (attempt {attempt+1}): {e} – retrying in {wait}s"
                            )
                            time.sleep(wait)

                            if attempt == MAX_RETRIES - 1:
                                raise

            logger.info("✅ BATCH SUCCESS (TOTAL)")
            logger.info(f"   Nodes created: {overall['nodes']}")
            logger.info(f"   Relationships created: {overall['rels']}")
            logger.info(f"   Properties set: {overall['props']}")

            # return fake summary-like object for CLI compatibility
            class S:
                counters = type("c", (), {
                    "nodes_created": overall["nodes"],
                    "relationships_created": overall["rels"],
                    "properties_set": overall["props"]
                })

            return S()

        except Exception as e:
            logger.error(f"💥 BATCH FAILED: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


# ---------------------------
# CLI support - FIXED
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Neo4j Driver CLI")
    parser.add_argument("--read", help="Read-only Cypher query")
    parser.add_argument("--write", help="Write Cypher query") 
    parser.add_argument("--query", help="Cypher query for --batch")
    parser.add_argument("--batch", help="JSON batch file")
    parser.add_argument("--params", type=json.loads, nargs='?', default={}, 
                       help="JSON params: '{\"key\":\"value\"}'")
    
    args = parser.parse_args()
    driver = Neo4jDriver()
    
    try:
        if args.read:
            result = driver.read_query(args.read, args.params)
            print(json.dumps(result, indent=2))
        elif args.write:
            result = driver.write_query(args.write, args.params)
            print(f"✅ Write complete: {result}")
        elif args.batch and args.query:
            batch_data = json.load(open(args.batch))
            logger.info(f"📦 Loaded {len(batch_data)} rows from {args.batch}")
            summary = driver.execute_batch(args.query, batch_data)
            print(f"\n🎉 BATCH COMPLETE!")
            print(f"   📈 Nodes created: {summary.counters.nodes_created}")
            print(f"   🔗 Relationships: {summary.counters.relationships_created}")
            print(f"   ⚙️  Properties: {summary.counters.properties_set}")
        else:
            parser.print_help()
            return 1
    except Exception as e:
        logger.error(f"❌ CLI ERROR: {str(e)}")
        return 1
    finally:
        driver.close()
        return 0

if __name__ == "__main__":
    exit(main())
