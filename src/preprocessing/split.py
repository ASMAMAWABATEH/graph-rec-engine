# src/preprocessing/split.py
import pandas as pd
from pathlib import Path
import logging
import yaml

# --- Logger setup ---
logger = logging.getLogger("splitter")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

# --- Load config ---
with open("configs/preprocessing.yaml", "r") as f:
    cfg = yaml.safe_load(f)

TEST_RATIO = cfg.get("test_ratio", 0.2)  # default 20%

class SessionSplitter:
    def __init__(self, test_ratio: float = TEST_RATIO):
        self.test_ratio = test_ratio

    def temporal_split(self, df: pd.DataFrame):
        """
        Split sessions based on the last timestamp per session.
        Returns train_df, test_df
        """
        # last timestamp per session
        session_max_ts = df.groupby("session_id")["timestamp"].max()
        session_max_ts = session_max_ts.sort_values()

        cutoff_idx = int(len(session_max_ts) * (1 - self.test_ratio))
        train_sessions = session_max_ts.iloc[:cutoff_idx].index
        test_sessions = session_max_ts.iloc[cutoff_idx:].index

        logger.info(f"Total sessions: {len(session_max_ts):,}")
        logger.info(f"Train sessions: {len(train_sessions):,}")
        logger.info(f"Test sessions: {len(test_sessions):,}")

        train_df = df[df["session_id"].isin(train_sessions)]
        test_df  = df[df["session_id"].isin(test_sessions)]

        # sanity check: no overlap
        assert set(train_sessions).isdisjoint(set(test_sessions)), "Train/test session overlap!"

        return train_df, test_df

    def save_split(self, train_df: pd.DataFrame, test_df: pd.DataFrame, output_dir: str):
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        train_file = output_path / "train_sessions.parquet"
        test_file  = output_path / "test_sessions.parquet"

        train_df.to_parquet(train_file, index=False)
        test_df.to_parquet(test_file, index=False)

        logger.info(f"Train data saved: {train_file} ({len(train_df):,} rows)")
        logger.info(f"Test data saved: {test_file} ({len(test_df):,} rows)")

        return train_file, test_file

# --- CLI runner ---
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Temporal train/test split for sessions")
    parser.add_argument("--input", type=str, required=True, help="Processed sessions Parquet file")
    parser.add_argument("--output", type=str, default="data/processed/", help="Output directory")
    args = parser.parse_args()

    df = pd.read_parquet(args.input)

    splitter = SessionSplitter()
    train_df, test_df = splitter.temporal_split(df)
    splitter.save_split(train_df, test_df, args.output)
