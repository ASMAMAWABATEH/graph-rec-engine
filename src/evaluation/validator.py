# src/evaluation/validator.py

import json
from typing import List
from tqdm import tqdm
import math

from src.inference.recommender import Recommender
from .metrics import (
    hit_rate_at_k,
    mrr_at_k,
    precision_at_k,
    recall_at_k,
)


def ndcg_at_k(recommended: List[int], ground_truth: int, k: int = 10) -> float:
    """
    Normalized Discounted Cumulative Gain for a single ground-truth item.
    Returns 1 / log2(rank + 1) if the item is present, else 0.
    """
    if ground_truth in recommended[:k]:
        rank = recommended.index(ground_truth) + 1  # ranks start at 1
        return 1.0 / math.log2(rank + 1)
    return 0.0


class Validator:
    def __init__(self, model_name="hsp", top_k=10):
        self.model_name = model_name
        self.top_k = top_k
        self.rec = Recommender(top_k=top_k)

    def evaluate(self, sessions: List[List[int]]):
        hits, mrrs, precisions, recalls, ndcgs = [], [], [], [], []

        for session in tqdm(sessions):
            if len(session) < 2:
                continue

            input_seq = session[:-1]
            target = session[-1]

            if self.model_name == "hsp":
                recs = self.rec.hsp_predict(input_seq)
            else:
                recs = self.rec.ric_predict(input_seq)

            hits.append(hit_rate_at_k(recs, target))
            mrrs.append(mrr_at_k(recs, target))
            precisions.append(precision_at_k(recs, target))
            recalls.append(recall_at_k(recs, target))
            ndcgs.append(ndcg_at_k(recs, target, k=self.top_k))

        return {
            "HitRate@K": sum(hits) / len(hits) if hits else 0.0,
            "MRR@K": sum(mrrs) / len(mrrs) if mrrs else 0.0,
            "Precision@K": sum(precisions) / len(precisions) if precisions else 0.0,
            "Recall@K": sum(recalls) / len(recalls) if recalls else 0.0,
            "nDCG@K": sum(ndcgs) / len(ndcgs) if ndcgs else 0.0,
        }

    def close(self):
        self.rec.close()


# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="Path to test sessions JSON")
    parser.add_argument("--model", choices=["hsp", "ric"], default="hsp")
    parser.add_argument("--top_k", type=int, default=10)

    args = parser.parse_args()

    sessions = json.load(open(args.data))

    validator = Validator(model_name=args.model, top_k=args.top_k)

    results = validator.evaluate(sessions)

    print("\n📊 Evaluation Results:")
    for k, v in results.items():
        print(f"{k}: {v:.4f}")

    validator.close()
