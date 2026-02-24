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
```

If Neo4j runs outside this repo, pass its import directory so `LOAD CSV` can see the files:

```bash
make build-graph NEO4J_IMPORT_DIR=/path/to/neo4j/import
```

After this, you can evaluate models or run tests:

```bash
make evaluate
make test
```

## Pipeline & Inference Scripts

Run end-to-end pipeline from one command (YAML-driven):

```bash
.venv/bin/python -m src.cli.pipeline --config configs/pipeline.yaml
```

Useful flags:

```bash
# Reuse saved intermediate artifacts (default true)
.venv/bin/python -m src.cli.pipeline --reuse-intermediate

# Skip reloading graph to Neo4j when already built
.venv/bin/python -m src.cli.pipeline --skip-graph-load

# Quick validation on a small subset before full eval
.venv/bin/python -m src.cli.pipeline --subset_validate_sessions 200
```

Run inference (single or batch) with latency tracking:

```bash
# single session
.venv/bin/python -m src.cli.inference --session 12 45 78 --model both --top_k 10

# batch file input
.venv/bin/python -m src.cli.inference --input data/test_session.json --model both
```

Optional time-decay on Neo4j edge weights (NEXT/CO_OCCURS):

```bash
.venv/bin/python -m src.cli.pipeline --edge_time_decay 0.05
.venv/bin/python -m src.cli.inference --edge_time_decay 0.05
```

## Experiments & Hyperparameter Tuning

Use the reproducible experiment workflow (defaults to `experiments/hsp_vs_ric.yaml`):

```bash
# 1) Hyperparameter tuning (writes results/tables/hyperparam_tuning.csv)
make tune

# 2) Final tuned evaluation for HSP + RIC
make final-eval

# 3) Build merged comparison table
make compare-eval
```

Or run everything end-to-end:

```bash
make experiment-all
```

Generate notebook-style visualization artifacts in one command:

```bash
make viz-smoke
```

Run with a different experiment file:

```bash
make tune EXPERIMENT=experiments/hyperparam_tuning.yaml
make final-eval EXPERIMENT=experiments/hsp_vs_ric.yaml
make compare-eval EXPERIMENT=experiments/hsp_vs_ric.yaml
```

Cleanup temporary files and cache:

```bash
make clean
```

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

    next_typed.csv

    contains_typed.csv

    cooccurs_typed.csv

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

## Project Structure

```text
graph-sbr-system/
├── configs/               # YAML configs for pipeline, inference, models, and evaluation
├── data/
│   ├── raw/               # Source Yoochoose files
│   ├── processed/         # Sessionized/split parquet artifacts
│   └── neo4j_import/      # Typed TSV/CSV files for Neo4j LOAD CSV
├── database/
│   ├── build_bulk.py      # Build import artifacts from training sessions
│   ├── load_graph.py      # Stage files and execute Cypher loaders
│   ├── validate_graph.py  # Post-load graph integrity checks
│   └── cypher/            # Schema + data loading queries
├── experiments/           # Reproducible experiment definitions
├── scripts/               # Tuning, final evaluation, comparison, and visualization runners
├── src/
│   ├── cli/               # Canonical CLI entrypoints
│   ├── preprocessing/     # Raw data ingestion, filtering, and temporal splitting
│   ├── inference/         # Recommender orchestration and cold-start logic
│   ├── evaluation/        # Metrics, validators, and hyperparameter tuning
│   └── models/            # HSP and RIC model implementations
├── tests/                 # Unit and integration tests
├── run_pipeline.py        # Backward-compatible pipeline wrapper
├── run_inference.py       # Backward-compatible inference wrapper
├── Makefile               # Task orchestration for setup, pipeline, eval, and QA
└── README.md
```
