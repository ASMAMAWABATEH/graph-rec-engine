# Graph-SBR: Session-Based Recommendation System

A **session-based recommender system** using **Neo4j graph database**. This project converts session logs into a graph structure, supports graph-based recommendations, and allows evaluation of models like **HSP** and **RIC**.

---

## Quick Start

Recommended bootstrap flow:

```bash
# 1) Setup environment and install dependencies
make setup

# 2) Verify Neo4j connectivity and env vars
make preflight

# 3) Ensure raw + processed data exist
# (provide RAW_DATA_URL if raw file is not already present)
make prepare-data RAW_DATA_URL=https://.../yoochoose-clicks.dat

# 4) Build Neo4j graph
make build-graph
```

If Neo4j runs outside this repo, pass its import directory so `LOAD CSV` can see the files:

```bash
make build-graph NEO4J_IMPORT_DIR=/path/to/neo4j/import
```

After graph load, evaluate models or run tests:

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

## Workflow Summary

1. Validate environment and connectivity:

```bash
make setup
make preflight
```

2. Prepare data artifacts:

```bash
# Option A: if RAW_DATA already exists locally
make prepare-data

# Option B: download RAW_DATA, then auto-generate PROCESSED_DATA
make prepare-data RAW_DATA_URL=https://.../yoochoose-clicks.dat

# Option C: directly download both files
make download-raw RAW_DATA_URL=https://.../yoochoose-clicks.dat
make download-processed PROCESSED_DATA_URL=https://.../yoochoose_sessions.parquet
```

3. Build graph artifacts and load Neo4j:

```bash
make build-bulk
make build-graph
```

4. Run evaluation and quality checks:

```bash
make evaluate
make test
```

5. Clean generated local artifacts:

```bash
make clean
```

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
├── scripts/               # Operational scripts (preflight, tuning, final eval, comparison, viz)
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
