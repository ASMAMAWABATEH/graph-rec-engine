# src/models/ric_model.py
from typing import List, Dict
from .base import BaseRecommender

class RICModel(BaseRecommender):
    """
    RIC: Recurrent Item Co-occurrence
    Scores items based on weighted co-occurrence with all items in the current session.
    """

    def __init__(self, next_edges: Dict[int, Dict[int, int]], decay: float = 0.7):
        """
        next_edges: dictionary {source_item_id: {target_item_id: weight}}
        decay: temporal decay factor for older session items
        """
        self.next_edges = next_edges
        self.decay = decay

    def predict(self, session_items: List[int], top_k: int = 10) -> List[int]:
        """
        Predict the next items given session_items.
        Returns top-K recommended internal item IDs.
        """
        scores = self.score(session_items)
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [item for item, _ in ranked[:top_k]]

    def score(self, session_items: List[int]) -> Dict[int, float]:
        """
        Compute RIC scores: sum weighted co-occurrences with decay for older items.
        """
        scores = {}
        n = len(session_items)
        for idx, item in enumerate(reversed(session_items)):
            weight = self.decay ** idx
            if item in self.next_edges:
                for target, w in self.next_edges[item].items():
                    scores[target] = scores.get(target, 0) + weight * w
        return scores

