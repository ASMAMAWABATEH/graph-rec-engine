# src/models/ric_model.py
from typing import List, Dict
from .base import BaseRecommender

class RICModel(BaseRecommender):
    """
    RIC: Recurrent Item Co-occurrence
    Scores items based on co-occurrence edges with optional temporal decay.
    """

    def __init__(self, next_edges: Dict[int, Dict[int, int]], decay: float = 0.7):
        """
        next_edges: dictionary {source_item_id: {target_item_id: weight}}
        decay: temporal decay factor for recent items
        """
        self.cooccurs = next_edges
        self.decay = decay

    def predict(
        self,
        session_items: List[int],
        top_k: int = 10,
        alpha: float = 1.0,
        beta: float = 0.5,
        gamma: float = 1.0
    ) -> List[int]:
        """
        Predict next items based on co-occurrences with weighted decay:
        - alpha: weight for most recent item
        - beta: weight for second-to-last item
        - gamma: temporal decay factor
        """
        scores = self.score(session_items, alpha=alpha, beta=beta, gamma=gamma)
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [item for item, _ in ranked[:top_k]]

    def score(
        self,
        session_items: List[int],
        alpha: float = 1.0,
        beta: float = 0.5,
        gamma: float = 0.7
    ) -> Dict[int, float]:
        if not session_items or not self.cooccurs:
            return {}

        scores = {}
        n = len(session_items)

        # last item
        last_item = session_items[-1]
        if last_item in self.cooccurs:
            for tgt, w in self.cooccurs[last_item].items():
                scores[tgt] = scores.get(tgt, 0) + alpha * float(w) * gamma**0

        # second-to-last item
        if n >= 2:
            prev_item = session_items[-2]
            if prev_item in self.cooccurs:
                for tgt, w in self.cooccurs[prev_item].items():
                    scores[tgt] = scores.get(tgt, 0) + beta * float(w) * gamma**1

        # avoid recommending items already in session
        for item in session_items:
            scores.pop(item, None)

        return scores