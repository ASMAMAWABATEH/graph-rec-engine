# src/models/ric_model.py
from typing import List, Dict
from .base import BaseRecommender

class RICModel(BaseRecommender):
    """
    RIC: Recurrent Item Co-occurrence
    Aggregates CO_OCCURS weights over all items in the session.
    """

    CO_OCCURS_QUERY = """
    MATCH (i:Item {item_id: $item})-[c:CO_OCCURS]-(n:Item)
    RETURN n.item_id AS item, c.weight AS w
    ORDER BY c.weight DESC
    LIMIT 200
    """

    def score(self, session_items: List[int]) -> Dict[int, float]:
        scores: Dict[int, float] = {}

        for item in session_items:
            rows = self.db.read_query(self.CO_OCCURS_QUERY, {"item": item})

            for r in rows:
                scores[r["item"]] = scores.get(r["item"], 0) + float(r["w"])

        return scores
