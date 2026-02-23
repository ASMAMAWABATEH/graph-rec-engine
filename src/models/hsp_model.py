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

    def predict(
        self,
        session_items: List[int],
        top_k: int = 10,
        alpha: float = 1.0,
        beta: float = 0.5,
        gamma: float = 1.0
    ) -> List[int]:
        """
        Predict the next items given session_items with weighting:
        - alpha: weight for last_item NEXT edges
        - beta: weight for second-to-last item (backoff)
        - gamma: recency / decay factor
        """
        scores = self.score(session_items, alpha=alpha, beta=beta, gamma=gamma)
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [item for item, _ in ranked[:top_k]]

    def score(
        self,
        session_items: List[int],
        alpha: float = 1.0,
        beta: float = 0.5,
        gamma: float = 1.0
    ) -> Dict[int, float]:
        if not session_items:
            return {}

        scores = {}
        n = len(session_items)

        # last item
        last_item = session_items[-1]
        if last_item in self.next_edges:
            for tgt, w in self.next_edges[last_item].items():
                scores[tgt] = scores.get(tgt, 0) + alpha * float(w) * gamma**0

        # second-to-last item (backoff)
        if n >= 2:
            prev_item = session_items[-2]
            if prev_item in self.next_edges:
                for tgt, w in self.next_edges[prev_item].items():
                    scores[tgt] = scores.get(tgt, 0) + beta * float(w) * gamma**1

        return scores