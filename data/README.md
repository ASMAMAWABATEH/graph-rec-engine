# Data Layout

- `raw/`: original Yoochoose source files (ignored by git in normal flow).
- `processed/`: parquet outputs from preprocessing/splitting.
- `neo4j_import/`: bulk CSV/TSV files used by Neo4j `LOAD CSV`.
- `batch.json`: optional intermediate session-event format.
- `test_session.json`: tiny local sample for inference/eval smoke runs.

Notes:
- Regenerate processed and Neo4j import artifacts via pipeline/make targets.
- Do not manually edit generated files under `processed/` and `neo4j_import/`.
