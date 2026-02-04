import pandas as pd
import json
from pathlib import Path

# Paths
input_parquet = Path("data/processed/yoochoose_sessions.parquet")
output_json = Path("data/batch.json")

# Load processed sessions
df = pd.read_parquet(input_parquet)

# Ensure data is sorted by session and position
df = df.sort_values(["session_id", "position"])

batch = []

# Iterate per session
for session_id, group in df.groupby("session_id"):
    items = group["item_id"].tolist()
    for i in range(len(items)):
        next_item = items[i + 1] if i + 1 < len(items) else None
        batch.append({
            "session_id": int(session_id),
            "item_id": int(items[i]),
            "next_item_id": int(next_item) if next_item is not None else None
        })

# Save JSON
output_json.parent.mkdir(exist_ok=True)
with open(output_json, "w") as f:
    json.dump(batch, f, indent=2)

print(f"✅ Batch JSON generated: {len(batch)} rows → {output_json}")
