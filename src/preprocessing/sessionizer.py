# src/preprocessing/sessionizer.py
import pandas as pd
from pathlib import Path

class Sessionizer:
    def __init__(
        self, 
        min_session_length: int = 2, 
        min_item_freq: int = 5, 
        chunksize: int = 500_000
    ):
        """
        Ultimate memory-efficient sessionizer.

        Args:
            min_session_length: remove sessions shorter than this
            min_item_freq: remove items appearing fewer times
            chunksize: rows per chunk for reading
        """
        self.min_session_length = min_session_length
        self.min_item_freq = min_item_freq
        self.chunksize = chunksize

    def load_raw_chunked(self, path: str) -> pd.DataFrame:
        """Load large raw YOOCHOOSE files in chunks (8GB RAM safe)"""
        all_chunks = []
        raw_path = Path(path)

        print(f"📖 Reading {path} with comma separator...")

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

            # Dev safety: stop at ~5M rows on 8GB RAM
            if i >= 9:
                print("🛑 Reached ~5 million rows (Dev Limit for 8GB RAM).")
                break

            print(f"Loaded chunk {i+1}: {len(chunk):,} rows")

        df = pd.concat(all_chunks, ignore_index=True)
        print(f"📊 Total rows loaded: {len(df):,}")
        print(f"Total sessions: {df['session_id'].nunique():,}")
        print(f"Total items: {df['item_id'].nunique():,}")
        return df

    def sessionize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sort events, filter short sessions, filter infrequent items,
        add position index
        """
        print("🔄 Sessionizing...")

        # Sort by session_id then timestamp
        df = df.sort_values(["session_id", "timestamp"])

        # 1️⃣ Remove short sessions
        session_lengths = df.groupby("session_id").size()
        valid_sessions = session_lengths[session_lengths >= self.min_session_length].index
        df = df[df["session_id"].isin(valid_sessions)]
        print(f"After session length filter: {len(df):,} events in {len(valid_sessions):,} sessions")

        # 2️⃣ Remove infrequent items
        item_counts = df.groupby("item_id").size()
        keep_items = item_counts[item_counts >= self.min_item_freq].index
        df = df[df["item_id"].isin(keep_items)]

        # 3️⃣ Re-check session lengths (some sessions may now be too short)
        session_lengths = df.groupby("session_id").size()
        valid_sessions = session_lengths[session_lengths >= self.min_session_length].index
        df = df[df["session_id"].isin(valid_sessions)]

        print(f"After item frequency filter: {len(df):,} events in {len(valid_sessions):,} sessions, {df['item_id'].nunique():,} items")

        # 4️⃣ Add position per session
        df["position"] = df.groupby("session_id").cumcount()

        return df[["session_id", "item_id", "timestamp", "position"]]

    def run_pipeline(self, raw_path: str, output_path: str):
        """Complete pipeline: load → sessionize → save as Parquet"""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        df = self.load_raw_chunked(raw_path)
        processed_df = self.sessionize(df)
        processed_df.to_parquet(output_path, index=False)

        print(f"💾 Saved {len(processed_df)} events to {output_path}")
        return processed_df


# Quick CLI runner
if __name__ == "__main__":
    sessionizer = Sessionizer(min_session_length=2, min_item_freq=5)
    sessionizer.run_pipeline(
        "data/raw/yoochoose-clicks.dat",
        "data/processed/yoochoose_sessions.parquet"
    )
