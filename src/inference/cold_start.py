# src/inference/cold_start.py
import json
import csv
from pathlib import Path
from collections import Counter

# Path to precomputed item lookup or Neo4j bulk CSVs
LOOKUP_FILE = Path("data/neo4j_import/lookup.json")
NEXT_TYPED_CSV = Path("data/neo4j_import/next_typed.csv")
ITEMS_CSV = Path("data/neo4j_import/items.csv")

# Preload global top-K items
_global_top_k = None

def _load_global_top_k():
    """
    Computes global top-K items based on NEXT transition weights.
    Fallback if session has no history.
    """
    global _global_top_k
    if _global_top_k is not None:
        return _global_top_k

    # Aggregate weights from NEXT relationships
    weights = Counter()

    if NEXT_TYPED_CSV.exists():
        with NEXT_TYPED_CSV.open(newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                try:
                    tgt = int(row["dst_item_id"])
                    weight = float(row["weight"])
                    weights[tgt] += weight
                except (TypeError, ValueError, KeyError):
                    continue

    # If CSV missing, fallback to item IDs from lookup
    if not weights and LOOKUP_FILE.exists():
        lookup = json.load(LOOKUP_FILE.open())
        weights.update({int(iid): 1 for iid in lookup["item_to_id"].values()})

    # Sort descending by weight
    _global_top_k = [item for item, _ in weights.most_common(100)]
    return _global_top_k

def global_top_k(k=10):
    """
    Return top-K globally popular items.
    """
    top_items = _load_global_top_k()
    return top_items[:k]

def category_top_k(category_map: dict, category: str, k=10):
    """
    Return top-K items for a specific category (optional advanced cold-start)
    category_map: {item_id: category_name}
    """
    items_in_category = [iid for iid, cat in category_map.items() if cat == category]
    # Just return first k for now; can be sorted by precomputed popularity
    return items_in_category[:k]
