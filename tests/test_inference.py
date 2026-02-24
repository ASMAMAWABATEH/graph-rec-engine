import csv
import json
from pathlib import Path

import pandas as pd
import pytest

from database.build_bulk import build_bulk_from_rows
from src.evaluation.validator import Validator
from src.inference import cold_start
from src.inference import recommender as recommender_module
from src.preprocessing.sessionizer import Sessionizer
from src.preprocessing.split import SessionSplitter


class DummyNeo4jDriver:
    def __init__(self, next_rows=None, co_rows=None):
        self._next_rows = next_rows or []
        self._co_rows = co_rows or []
        self.closed = False

    def read_query(self, query: str, params=None):
        if "CO_OCCURS" in query:
            return self._co_rows
        if "NEXT" in query:
            return self._next_rows
        return []

    def close(self):
        self.closed = True


class ParamCaptureDriver(DummyNeo4jDriver):
    def __init__(self):
        super().__init__(next_rows=[], co_rows=[])
        self.params_seen = []

    def read_query(self, query: str, params=None):
        self.params_seen.append(params or {})
        return []


def _read_typed_rows(path: Path, src_key: str, dst_key: str):
    rows = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rows.append(
                {
                    "src": int(row[src_key]),
                    "dst": int(row[dst_key]),
                    "w": float(row["weight"]),
                }
            )
    return rows


def test_cold_start_prefers_next_typed_csv(tmp_path: Path, monkeypatch):
    next_path = tmp_path / "next_typed.csv"
    lookup_path = tmp_path / "lookup.json"

    next_path.write_text(
        "\n".join(
            [
                "src_item_id\tdst_item_id\tweight",
                "1\t101\t5",
                "2\t102\t8",
                "3\t101\t1",
            ]
        )
    )
    lookup_path.write_text(json.dumps({"item_to_id": {"10": 10, "20": 20}}))

    monkeypatch.setattr(cold_start, "NEXT_TYPED_CSV", next_path)
    monkeypatch.setattr(cold_start, "LOOKUP_FILE", lookup_path)
    monkeypatch.setattr(cold_start, "_global_top_k", None)

    assert cold_start.global_top_k(2) == [102, 101]


def test_cold_start_fallback_to_lookup(tmp_path: Path, monkeypatch):
    next_path = tmp_path / "missing_next_typed.csv"
    lookup_path = tmp_path / "lookup.json"
    lookup_path.write_text(json.dumps({"item_to_id": {"10": 10, "20": 20}}))

    monkeypatch.setattr(cold_start, "NEXT_TYPED_CSV", next_path)
    monkeypatch.setattr(cold_start, "LOOKUP_FILE", lookup_path)
    monkeypatch.setattr(cold_start, "_global_top_k", None)

    top = cold_start.global_top_k(5)
    assert set(top) == {10, 20}


def test_recommender_predict_and_empty_fallback(monkeypatch):
    next_rows = [
        {"src": 1, "dst": 2, "w": 2.0},
        {"src": 2, "dst": 3, "w": 4.0},
        {"src": 2, "dst": 4, "w": 1.0},
    ]
    co_rows = [
        {"src": 2, "dst": 3, "w": 3.0},
        {"src": 2, "dst": 4, "w": 2.0},
    ]
    driver = DummyNeo4jDriver(next_rows=next_rows, co_rows=co_rows)

    monkeypatch.setattr(recommender_module, "Neo4jDriver", lambda: driver)
    monkeypatch.setattr(recommender_module.Recommender, "_load_lookup", lambda self: None)
    monkeypatch.setattr(recommender_module, "global_top_k", lambda k: list(range(100, 100 - k, -1)))

    rec = recommender_module.Recommender(top_k=3)
    try:
        assert rec.hsp_predict([]) == [100, 99, 98]
        assert rec.ric_predict([]) == [100, 99, 98]

        hsp = rec.hsp_predict([1, 2])
        ric = rec.ric_predict([1, 2])
        assert hsp[:2] == [3, 4]
        assert ric[:2] == [3, 4]
    finally:
        rec.close()
    assert driver.closed is True


def test_recommender_nonempty_session_falls_back_when_graph_has_no_edges(monkeypatch):
    driver = DummyNeo4jDriver(next_rows=[], co_rows=[])

    monkeypatch.setattr(recommender_module, "Neo4jDriver", lambda: driver)
    monkeypatch.setattr(recommender_module.Recommender, "_load_lookup", lambda self: None)
    monkeypatch.setattr(recommender_module, "global_top_k", lambda k: [901 + i for i in range(k)])

    rec = recommender_module.Recommender(top_k=3)
    try:
        assert rec.hsp_predict([111, 222]) == [901, 902, 903]
        assert rec.ric_predict([111, 222]) == [901, 902, 903]
    finally:
        rec.close()


def test_recommender_passes_edge_time_decay_param(monkeypatch):
    driver = ParamCaptureDriver()
    monkeypatch.setattr(recommender_module, "Neo4jDriver", lambda: driver)
    monkeypatch.setattr(recommender_module.Recommender, "_load_lookup", lambda self: None)

    rec = recommender_module.Recommender(top_k=3, edge_time_decay_lambda=0.25)
    try:
        assert driver.params_seen
        assert all(params.get("lambda") == 0.25 for params in driver.params_seen if params)
    finally:
        rec.close()


def test_recommender_transition_mix_probability_mode(monkeypatch):
    next_rows = [
        {"src": 1, "dst": 2, "w": 8.0},
        {"src": 1, "dst": 3, "w": 2.0},
    ]
    driver = DummyNeo4jDriver(next_rows=next_rows, co_rows=[])

    monkeypatch.setattr(recommender_module, "Neo4jDriver", lambda: driver)
    monkeypatch.setattr(recommender_module.Recommender, "_load_lookup", lambda self: None)

    rec = recommender_module.Recommender(top_k=3, transition_mix=1.0)
    try:
        assert rec.next_edges[1][2] == pytest.approx(0.8)
        assert rec.next_edges[1][3] == pytest.approx(0.2)
    finally:
        rec.close()


def test_recommender_transition_mix_hybrid_mode(monkeypatch):
    next_rows = [
        {"src": 1, "dst": 2, "w": 8.0},
        {"src": 1, "dst": 3, "w": 2.0},
    ]
    driver = DummyNeo4jDriver(next_rows=next_rows, co_rows=[])

    monkeypatch.setattr(recommender_module, "Neo4jDriver", lambda: driver)
    monkeypatch.setattr(recommender_module.Recommender, "_load_lookup", lambda self: None)

    rec = recommender_module.Recommender(top_k=3, transition_mix=0.5)
    try:
        assert rec.next_edges[1][2] == pytest.approx(0.9)
        assert rec.next_edges[1][3] == pytest.approx(0.225)
    finally:
        rec.close()


def test_integration_preprocess_build_predict_evaluate(tmp_path: Path, monkeypatch):
    # 1) Preprocess a small reproducible raw dataset.
    raw_path = tmp_path / "raw.csv"
    raw_path.write_text(
        "\n".join(
            [
                "1,2024-01-01T00:00:01,1,cat",
                "1,2024-01-01T00:00:02,2,cat",
                "1,2024-01-01T00:00:03,3,cat",
                "2,2024-01-02T00:00:01,1,cat",
                "2,2024-01-02T00:00:02,2,cat",
                "2,2024-01-02T00:00:03,4,cat",
                "3,2024-01-03T00:00:01,1,cat",
                "3,2024-01-03T00:00:02,2,cat",
                "3,2024-01-03T00:00:03,3,cat",
                "4,2024-01-04T00:00:01,1,cat",
                "4,2024-01-04T00:00:02,2,cat",
                "4,2024-01-04T00:00:03,4,cat",
            ]
        )
    )

    sessionizer = Sessionizer(min_session_length=2, min_item_freq=1, chunksize=100, max_chunks=1)
    loaded = sessionizer.load_raw_chunked(str(raw_path))
    processed = sessionizer.sessionize(loaded)

    splitter = SessionSplitter(test_ratio=0.5)
    train_df, test_df = splitter.temporal_split(processed)

    # 2) Build graph bulk artifacts from train sessions.
    batch_rows = []
    for session_id, grp in train_df.sort_values(["session_id", "position"]).groupby("session_id"):
        items = grp["item_id"].tolist()
        for idx, item in enumerate(items):
            nxt = items[idx + 1] if idx + 1 < len(items) else None
            batch_rows.append(
                {
                    "session_id": int(session_id),
                    "item_id": int(item),
                    "next_item_id": int(nxt) if nxt is not None else None,
                }
            )

    out_dir = tmp_path / "neo4j_import"
    stats = build_bulk_from_rows(batch_rows, out_dir)
    assert stats["items"] == 4
    assert stats["next_edges"] >= 3
    assert stats["cooccurs_edges"] >= 3

    next_rows = _read_typed_rows(out_dir / "next_typed.csv", "src_item_id", "dst_item_id")
    co_rows = _read_typed_rows(out_dir / "cooccurs_typed.csv", "src_item_id", "dst_item_id")

    # 3) Predict and evaluate using a deterministic fake Neo4j driver.
    driver = DummyNeo4jDriver(next_rows=next_rows, co_rows=co_rows)
    monkeypatch.setattr(recommender_module, "Neo4jDriver", lambda: driver)
    monkeypatch.setattr(recommender_module.Recommender, "_load_lookup", lambda self: None)

    rec = recommender_module.Recommender(top_k=5)
    try:
        eval_sessions = (
            test_df.sort_values(["session_id", "position"])
            .groupby("session_id")["item_id"]
            .apply(list)
            .tolist()
        )

        # Ensure predict path works.
        sample_input = eval_sessions[0][:-1]
        assert rec.hsp_predict(sample_input)
        assert rec.ric_predict(sample_input)

        # Ensure evaluate path works end-to-end.
        validator = Validator(top_k=5, recommender=rec)
        hsp_metrics = validator.evaluate_model(eval_sessions, "hsp")
        ric_metrics = validator.evaluate_model(eval_sessions, "ric")
    finally:
        rec.close()

    assert 0.0 <= hsp_metrics["MicroMetrics"]["HitRate@K"] <= 1.0
    assert 0.0 <= ric_metrics["MicroMetrics"]["HitRate@K"] <= 1.0
    assert hsp_metrics["MicroMetrics"]["HitRate@K"] > 0.0
