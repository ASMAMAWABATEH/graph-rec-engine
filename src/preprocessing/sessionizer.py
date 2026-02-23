# src/preprocessing/sessionizer.py
import pandas as pd
from pathlib import Path
from typing import Optional
from src.utils.logger import get_logger


logger = get_logger(__name__)

class Sessionizer:
    def __init__(
        self, 
        min_session_length: int = 2, 
        min_item_freq: int = 5, 
        chunksize: int = 500_000,
        max_chunks: Optional[int] = None,
    ):
        """
        Ultimate memory-efficient sessionizer.

        Args:
            min_session_length: remove sessions shorter than this
            min_item_freq: remove items appearing fewer times
            chunksize: rows per chunk for reading
            max_chunks: optional dev cap to stop early
        """
        self.min_session_length = min_session_length
        self.min_item_freq = min_item_freq
        self.chunksize = chunksize
        self.max_chunks = max_chunks

    def load_raw_chunked(self, path: str) -> pd.DataFrame:
        """Load large raw YOOCHOOSE files in chunks (8GB RAM safe)"""
        all_chunks = []
        raw_path = Path(path)

        logger.info("Reading %s with comma separator", path)

        for i, chunk in enumerate(pd.read_csv(
            raw_path,
            sep=',',  # YOOCHOOSE files are comma-separated
            chunksize=self.chunksize,
            names=["session_id", "timestamp", "item_id", "category"],
            dtype={'session_id': 'object', 'item_id': 'object'},
            on_bad_lines='warn'
        )):
            # Drop missing values
            chunk = chunk.dropna(subset=['session_id', 'item_id', 'timestamp'])

            # Keep only numeric IDs
            chunk = chunk[chunk['session_id'].str.isnumeric()]
            chunk = chunk[chunk['item_id'].str.isnumeric()]

            # Cast to int32
            chunk['session_id'] = chunk['session_id'].astype('int32')
            chunk['item_id'] = chunk['item_id'].astype('int32')

            # Parse timestamp
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], errors='coerce')
            chunk = chunk.dropna(subset=['timestamp'])

            all_chunks.append(chunk)

            if self.max_chunks is not None and (i + 1) >= self.max_chunks:
                logger.info("Reached chunk cap (%s). Stopping early.", self.max_chunks)
                break

            logger.info("Loaded chunk %s: %s rows", i + 1, f"{len(chunk):,}")

        df = pd.concat(all_chunks, ignore_index=True)
        logger.info("Total rows loaded: %s", f"{len(df):,}")
        logger.info("Total sessions: %s", f"{df['session_id'].nunique():,}")
        logger.info("Total items: %s", f"{df['item_id'].nunique():,}")
        return df

    def sessionize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sort events, filter short sessions, filter infrequent items,
        add position index
        """
        logger.info("Sessionizing...")

        # Sort by session_id then timestamp
        df = df.sort_values(["session_id", "timestamp"])

        # 1️⃣ Remove short sessions
        session_lengths = df.groupby("session_id").size()
        valid_sessions = session_lengths[session_lengths >= self.min_session_length].index
        df = df[df["session_id"].isin(valid_sessions)]
        logger.info(
            "After session length filter: %s events in %s sessions",
            f"{len(df):,}",
            f"{len(valid_sessions):,}",
        )

        # 2️⃣ Remove infrequent items
        item_counts = df.groupby("item_id").size()
        keep_items = item_counts[item_counts >= self.min_item_freq].index
        df = df[df["item_id"].isin(keep_items)]

        # 3️⃣ Re-check session lengths (some sessions may now be too short)
        session_lengths = df.groupby("session_id").size()
        valid_sessions = session_lengths[session_lengths >= self.min_session_length].index
        df = df[df["session_id"].isin(valid_sessions)]

        logger.info(
            "After item frequency filter: %s events in %s sessions, %s items",
            f"{len(df):,}",
            f"{len(valid_sessions):,}",
            f"{df['item_id'].nunique():,}",
        )

        # 4️⃣ Add position per session
        df["position"] = df.groupby("session_id").cumcount()

        return df[["session_id", "item_id", "timestamp", "position"]]

    def run_pipeline(self, raw_path: str, output_path: str):
        """Complete pipeline: load → sessionize → save as Parquet"""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        df = self.load_raw_chunked(raw_path)
        processed_df = self.sessionize(df)
        processed_df.to_parquet(output_path, index=False)

        logger.info("Saved %s events to %s", f"{len(processed_df):,}", output_path)
        return processed_df


# Quick CLI runner
if __name__ == "__main__":
    sessionizer = Sessionizer(min_session_length=2, min_item_freq=5)
    sessionizer.run_pipeline(
        "data/raw/yoochoose-clicks.dat",
        "data/processed/yoochoose_sessions.parquet"
    )
