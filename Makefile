# Makefile for Graph-SBR System
# Maintains phases: Preprocessing → Graph Construction → Evaluation → Testing

# --- Configuration ---
PYTHON := .venv/bin/python3
PIP := .venv/bin/pip
RAW_DATA := data/raw/yoochoose-clicks.dat
PROCESSED_DATA := data/processed/yoochoose_sessions.parquet
NEO4J_IMPORT := data/neo4j_import
NEO4J_IMPORT_DIR ?=
EXPERIMENT ?= experiments/hsp_vs_ric.yaml
TEST_REPORT_DIR := results/tests
PIPELINE_CONFIG ?= configs/pipeline.yaml
INFERENCE_CONFIG ?= configs/inference.yaml
INFER_INPUT ?= data/test_session.json
INFER_MODEL ?= both
INFER_TOP_K ?= 10
PIPELINE_ARGS ?=
INFER_ARGS ?=

# --- Help Menu ---
.PHONY: help setup preprocess build-bulk build-graph pipeline infer evaluate tune final-eval compare-eval experiment-all viz-smoke test clean

help:
	@echo "Graph-SBR System Command Menu:"
	@echo "  make setup         - Install dependencies and prepare environment"
	@echo "  make preprocess    - Run sessionization and temporal splitting"
	@echo "  make build-bulk    - Generate Neo4j bulk CSVs from batch.json"
	@echo "  make build-graph   - Initialize Neo4j schema and load training data"
	@echo "  make pipeline      - Run full YAML-driven pipeline (src.cli.pipeline)"
	@echo "  make infer         - Run YAML-driven inference (src.cli.inference)"
	@echo "  make evaluate      - Run model evaluation (Recall@K, MRR@K)"
	@echo "  make tune          - Run hyperparameter tuning from EXPERIMENT YAML"
	@echo "  make final-eval    - Run final tuned eval from EXPERIMENT YAML"
	@echo "  make compare-eval  - Build final HSP vs RIC comparison CSV"
	@echo "  make experiment-all - Run tune + final-eval + compare-eval"
	@echo "  make viz-smoke     - Generate notebook visualization artifacts"
	@echo "  make test          - Run all integrity tests"
	@echo "  make clean         - Remove temporary files and pycache"

# --- Environment Setup ---
setup:
	@echo "🔧 Setting up environment..."
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env - please update credentials"; fi

# --- Phase 1: Data Orchestration ---
preprocess:
	@echo "📊 Running Phase 1: Preprocessing..."
	$(PYTHON) src/preprocessing/sessionizer.py
	$(PYTHON) src/preprocessing/split.py --input $(PROCESSED_DATA) --output data/processed/

# --- Phase 2: Bulk CSV Generation ---
build-bulk: $(NEO4J_IMPORT)
	@echo "📦 Generating Neo4j bulk CSVs..."
	$(PYTHON) database/build_bulk.py

# Create Neo4j import folder if missing
$(NEO4J_IMPORT):
	@mkdir -p $(NEO4J_IMPORT)

# --- Phase 2: Graph Construction ---
build-graph: build-bulk
	@echo "🌐 Building Neo4j graph..."
	$(PYTHON) database/load_graph.py $(if $(NEO4J_IMPORT_DIR),--neo4j-import-dir $(NEO4J_IMPORT_DIR),)
	$(PYTHON) database/validate_graph.py --source-dir $(NEO4J_IMPORT)

# --- Phase 3 & 4: Evaluation ---
pipeline:
	@echo "🚀 Running full pipeline using $(PIPELINE_CONFIG)..."
	$(PYTHON) -m src.cli.pipeline --config $(PIPELINE_CONFIG) $(PIPELINE_ARGS)

infer:
	@echo "🔮 Running inference using $(INFERENCE_CONFIG)..."
	$(PYTHON) -m src.cli.inference --config $(INFERENCE_CONFIG) --input $(INFER_INPUT) --model $(INFER_MODEL) --top_k $(INFER_TOP_K) $(INFER_ARGS)

evaluate:
	@echo "📈 Running Phase 3 & 4: Model Evaluation..."
	$(PYTHON) -m src.cli.pipeline --mode eval --model hsp
	$(PYTHON) -m src.cli.pipeline --mode eval --model ric

tune:
	@echo "🧠 Running hyperparameter tuning using $(EXPERIMENT)..."
	$(PYTHON) scripts/run_tuning_experiment.py --experiment $(EXPERIMENT)

final-eval:
	@echo "🏁 Running final tuned evaluation using $(EXPERIMENT)..."
	$(PYTHON) scripts/final_tuned_evaluation.py --experiment $(EXPERIMENT)

compare-eval:
	@echo "📊 Building final evaluation comparison using $(EXPERIMENT)..."
	$(PYTHON) scripts/compare_final_eval.py --experiment $(EXPERIMENT)

experiment-all: tune final-eval compare-eval
	@echo "✅ Experiment workflow complete for $(EXPERIMENT)"

viz-smoke:
	@echo "🖼️ Generating visualization smoke artifacts..."
	$(PYTHON) scripts/viz_smoke.py --output_dir results/figures

# --- Quality Assurance ---
test:
	@echo "🧪 Running tests..."
	@mkdir -p $(TEST_REPORT_DIR)
	$(PYTHON) -m pytest tests/ -q --maxfail=1 --disable-warnings --junitxml=$(TEST_REPORT_DIR)/pytest.xml

# --- Cleanup ---
clean:
	@echo "🧹 Cleaning temporary files..."
	rm -rf data/processed/*.parquet
	rm -rf data/processed/*.csv
	rm -rf $(NEO4J_IMPORT)/*
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Cleanup complete."
