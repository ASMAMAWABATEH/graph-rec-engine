from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

with driver.session(database=os.getenv("NEO4J_DATABASE")) as session:
    result = session.run("RETURN 1 AS ok")
    print("Neo4j reachable:", result.single()["ok"] == 1)

driver.close()
