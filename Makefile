# Makefile for Graph-SBR System
# Maintains phases: Preprocessing → Graph Construction → Evaluation → Testing

# --- Configuration ---
PYTHON := .venv/bin/python3
PIP := .venv/bin/pip
RAW_DATA := data/raw/yoochoose-clicks.dat
PROCESSED_DATA := data/processed/yoochoose_sessions.parquet
NEO4J_IMPORT := data/neo4j_import
NEO4J_CLI := cypher-shell
NEO4J_USER := neo4j
NEO4J_PASS := mypassword
NEO4J_DB := graph_sbr

# --- Help Menu ---
.PHONY: help setup preprocess build-bulk build-graph evaluate test clean

help:
	@echo "Graph-SBR System Command Menu:"
	@echo "  make setup         - Install dependencies and prepare environment"
	@echo "  make preprocess    - Run sessionization and temporal splitting"
	@echo "  make build-bulk    - Generate Neo4j bulk CSVs from batch.json"
	@echo "  make build-graph   - Initialize Neo4j schema and load training data"
	@echo "  make evaluate      - Run model evaluation (Recall@K, MRR@K)"
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
	# Apply schema (constraints + indexes)
	$(NEO4J_CLI) -u $(NEO4J_USER) -p $(NEO4J_PASS) --database=$(NEO4J_DB) < database/cypher/schema.cypher
	# Load main graph (nodes + NEXT + CONTAINS)
	$(NEO4J_CLI) -u $(NEO4J_USER) -p $(NEO4J_PASS) --database=$(NEO4J_DB) < database/cypher/build.cypher
	# Optional: build CO_OCCURS relationships
	$(NEO4J_CLI) -u $(NEO4J_USER) -p $(NEO4J_PASS) --database=$(NEO4J_DB) < database/cypher/cooccurs.cypher

# --- Phase 3 & 4: Evaluation ---
evaluate:
	@echo "📈 Running Phase 3 & 4: Model Evaluation..."
	$(PYTHON) run_pipeline.py --mode eval --model hsp
	$(PYTHON) run_pipeline.py --mode eval --model ric

# --- Quality Assurance ---
test:
	@echo "🧪 Running tests..."
	$(PYTHON) -m pytest tests/

# --- Cleanup ---
clean:
	@echo "🧹 Cleaning temporary files..."
	rm -rf data/processed/*.parquet
	rm -rf data/processed/*.csv
	rm -rf $(NEO4J_IMPORT)/*
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Cleanup complete."
