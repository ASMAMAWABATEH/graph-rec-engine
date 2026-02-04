from abc import ABC, abstractmethod
from typing import List, Dict
import pandas as pd
from database.driver import Neo4jDriver


class BaseRecommender(ABC):
    """
    Research-grade interface:
    score → normalize → recommend
    """

    def __init__(self, driver: Neo4jDriver = None):
        # Allow DI for testing, fallback to owned driver
        self.db = driver or Neo4jDriver()

    # ----- Core Theory API -----

    @abstractmethod
    def score(self, session_items: List[int]) -> Dict[int, float]:
        """Raw graph scores before normalization"""
        pass

    def normalize(self, scores: Dict[int, float]) -> Dict[int, float]:
        s = sum(scores.values())
        return scores if s == 0 else {k: v / s for k, v in scores.items()}

    def recommend(self, session_items: List[int], k: int = 20) -> pd.DataFrame:
        if not session_items:
            return pd.DataFrame(columns=["item_id", "score"])

        scores = self.normalize(self.score(session_items))

        # avoid re-recommending items already in session
        for i in session_items:
            scores.pop(i, None)

        df = (
            pd.DataFrame(
                [{"item_id": i, "score": s} for i, s in scores.items()]
            )
            .sort_values("score", ascending=False)
            .head(k)
            .reset_index(drop=True)
        )
        return df

    def close(self):
        if self.db:
            self.db.close()
