# src/models/hsp_model.py
from typing import List, Dict
from .base import BaseRecommender

class HSPModel(BaseRecommender):
    """
    HSP: Hierarchical Sequence Probability
    Scores next items based on hierarchical probabilities from the NEXT edges in the graph.
    """

    def __init__(self, next_edges: Dict[int, Dict[int, int]]):
        """
        next_edges: dictionary {source_item_id: {target_item_id: weight}}
        """
        self.next_edges = next_edges

    def predict(self, session_items: List[int], top_k: int = 10) -> List[int]:
        """
        Predict the next items given session_items.
        Returns top-K recommended internal item IDs.
        """
        scores = self.score(session_items)
        # Sort by descending score
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [item for item, _ in ranked[:top_k]]

    def score(self, session_items: List[int]) -> Dict[int, float]:
        """
        Compute scores for all candidate next items.
        Uses last item in session and applies hierarchical back-off if needed.
        """
        if not session_items:
            return {}

        last_item = session_items[-1]
        scores = {}

        # Direct NEXT edges
        if last_item in self.next_edges:
            scores.update({i2: float(w) for i2, w in self.next_edges[last_item].items()})

        # Optional: Back-off to second-to-last item
        if len(session_items) >= 2:
            prev_item = session_items[-2]
            if prev_item in self.next_edges:
                for i2, w in self.next_edges[prev_item].items():
                    scores[i2] = scores.get(i2, 0) + 0.5 * float(w)  # decay weight

        return scores
