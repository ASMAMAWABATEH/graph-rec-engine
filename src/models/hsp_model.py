# src/models/hsp_model.py

from typing import List, Dict
from .base import BaseRecommender

class HSPModel(BaseRecommender):
    """
    HSP: Hierarchical Sequence Probability
    Uses NEXT relationships in the graph to score next-item probabilities.
    """

    NEXT_QUERY = """
    MATCH (i:Item {item_id: $item})-[r:NEXT]->(n:Item)
    RETURN n.item_id AS item, r.weight AS w
    ORDER BY r.weight DESC
    LIMIT 200
    """

    def score(self, session_items: List[int]) -> Dict[int, float]:
        if not session_items:
            return {}

        last_item = session_items[-1]

        rows = self.db.read_query(self.NEXT_QUERY, {"item": last_item})
        scores = {r["item"]: float(r["w"]) for r in rows}

        return scores
