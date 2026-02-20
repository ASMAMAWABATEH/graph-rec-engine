# Graph-SBR: Session-Based Recommendation System

A **session-based recommender system** using **Neo4j graph database**. This project converts session logs into a graph structure, supports graph-based recommendations, and allows evaluation of models like **HSP** and **RIC**.

---

## Quick Start

Run the entire pipeline with three commands:

```bash
# 1) Setup environment and install dependencies
make setup

# 2) Preprocess data
make preprocess

# 3) Build Neo4j graph
make build-graph

After this, you can evaluate models or run tests:

make evaluate
make test

Cleanup temporary files and cache:

make clean

Project Phases
Phase 1: Data Orchestration

    Preprocessing sessions from raw logs

    Temporal splitting for training and test sets

make preprocess

Outputs processed session files into data/processed/.
Phase 2: Graph Construction

    Generate Neo4j Bulk CSVs:

make build-bulk

Output folder: data/neo4j_import/ containing:

    items.csv

    sessions.csv

    next.csv

    contains.csv

    lookup.json

    Load Graph into Neo4j:

make build-graph

This will:

    Apply schema constraints and indexes (database/cypher/schema.cypher)

    Load nodes and relationships from bulk CSVs

    Optionally build CO_OCCURS relationships (database/cypher/cooccurs.cypher)

    Note: build-graph depends on build-bulk.

Phase 3 & 4: Evaluation

Evaluate recommendation models (HSP, RIC):

make evaluate

Metrics include Recall@K and MRR@K.
Testing

Run quality assurance tests:

make test

Uses pytest on the tests/ folder.
Cleanup

Remove temporary files, CSVs, and Python cache:

make clean

Folder Structure (Key Directories)

graph-sbr-system/
├─ data/
│  ├─ raw/                 # Raw session data
│  ├─ processed/           # Preprocessed session files
│  └─ neo4j_import/        # Neo4j bulk CSV outputs
├─ database/
│  ├─ build_bulk.py        # Generates bulk CSVs
│  └─ cypher/              # Neo4j Cypher scripts
├─ src/                     # Preprocessing scripts
├─ tests/                   # Test suite
├─ run_pipeline.py          # Main pipeline runner
├─ Makefile                 # Orchestrates phases
└─ README.md