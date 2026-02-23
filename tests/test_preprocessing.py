from pathlib import Path

import pandas as pd

from src.preprocessing.sessionizer import Sessionizer
from src.preprocessing.split import SessionSplitter


def test_sessionize_filters_and_positions():
    df = pd.DataFrame(
        [
            {"session_id": 1, "timestamp": "2024-01-01 00:00:02", "item_id": 20},
            {"session_id": 1, "timestamp": "2024-01-01 00:00:01", "item_id": 10},
            {"session_id": 2, "timestamp": "2024-01-02 00:00:01", "item_id": 10},
            {"session_id": 2, "timestamp": "2024-01-02 00:00:02", "item_id": 10},
            {"session_id": 3, "timestamp": "2024-01-03 00:00:01", "item_id": 30},
            {"session_id": 3, "timestamp": "2024-01-03 00:00:02", "item_id": 40},
            {"session_id": 4, "timestamp": "2024-01-04 00:00:01", "item_id": 20},
            {"session_id": 4, "timestamp": "2024-01-04 00:00:02", "item_id": 20},
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    sessionizer = Sessionizer(min_session_length=2, min_item_freq=2)
    out = sessionizer.sessionize(df)

    assert set(out["session_id"].unique()) == {1, 2, 4}
    assert set(out["item_id"].unique()) == {10, 20}

    for _, grp in out.groupby("session_id"):
        assert grp["position"].tolist() == list(range(len(grp)))


def test_load_raw_chunked_respects_max_chunks(tmp_path: Path):
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "\n".join(
            [
                "1,2024-01-01T00:00:01,10,cat",
                "1,2024-01-01T00:00:02,20,cat",
                "2,2024-01-01T00:00:01,30,cat",
                "2,2024-01-01T00:00:02,40,cat",
            ]
        )
    )

    sessionizer = Sessionizer(chunksize=2, max_chunks=1, min_session_length=1, min_item_freq=1)
    out = sessionizer.load_raw_chunked(str(raw))

    assert len(out) == 2
    assert set(out["session_id"].unique()) == {1}


def test_temporal_split_and_save(tmp_path: Path):
    df = pd.DataFrame(
        [
            {"session_id": 1, "timestamp": "2024-01-01 00:00:01", "item_id": 10, "position": 0},
            {"session_id": 1, "timestamp": "2024-01-01 00:00:02", "item_id": 20, "position": 1},
            {"session_id": 2, "timestamp": "2024-01-02 00:00:01", "item_id": 10, "position": 0},
            {"session_id": 2, "timestamp": "2024-01-02 00:00:02", "item_id": 30, "position": 1},
            {"session_id": 3, "timestamp": "2024-01-03 00:00:01", "item_id": 20, "position": 0},
            {"session_id": 3, "timestamp": "2024-01-03 00:00:02", "item_id": 40, "position": 1},
            {"session_id": 4, "timestamp": "2024-01-04 00:00:01", "item_id": 20, "position": 0},
            {"session_id": 4, "timestamp": "2024-01-04 00:00:02", "item_id": 50, "position": 1},
            {"session_id": 5, "timestamp": "2024-01-05 00:00:01", "item_id": 30, "position": 0},
            {"session_id": 5, "timestamp": "2024-01-05 00:00:02", "item_id": 60, "position": 1},
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    splitter = SessionSplitter(test_ratio=0.4)
    train_df, test_df = splitter.temporal_split(df)

    train_sessions = set(train_df["session_id"].unique())
    test_sessions = set(test_df["session_id"].unique())
    assert train_sessions.isdisjoint(test_sessions)
    assert len(train_sessions) == 3
    assert len(test_sessions) == 2

    out_dir = tmp_path / "processed"
    train_file, test_file = splitter.save_split(train_df, test_df, str(out_dir))
    assert train_file.exists()
    assert test_file.exists()
