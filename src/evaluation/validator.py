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
    ndcg_at_k
)


class Validator:
    def __init__(self, model_name="hsp", top_k=10, recommender=None):
        self.model_name = model_name
        self.top_k = top_k
        self.rec = recommender or Recommender(top_k=top_k)
        self._owns_rec = recommender is None

    def evaluate_model(self, sessions: List[List[int]], model_name: str):
        """Evaluate a single model on the test sessions"""
        hits, mrrs, precisions, recalls, ndcgs = [], [], [], [], []
        per_session_results = []

        for session in tqdm(sessions, desc=f"Evaluating {model_name.upper()}"):
            if len(session) < 2:
                continue

            input_seq = session[:-1]
            target = session[-1]

            recs = self.rec.hsp_predict(input_seq) if model_name == "hsp" else self.rec.ric_predict(input_seq)

            hit = hit_rate_at_k(recs, target)
            mrr = mrr_at_k(recs, target)
            precision = precision_at_k(recs, target)
            recall = recall_at_k(recs, target)
            ndcg = ndcg_at_k(recs, target, k=self.top_k)

            hits.append(hit)
            mrrs.append(mrr)
            precisions.append(precision)
            recalls.append(recall)
            ndcgs.append(ndcg)

            per_session_results.append({
                "hit": hit,
                "mrr": mrr,
                "precision": precision,
                "recall": recall,
                "ndcg": ndcg
            })

        micro_metrics = {
            "HitRate@K": sum(hits) / len(hits) if hits else 0.0,
            "MRR@K": sum(mrrs) / len(mrrs) if mrrs else 0.0,
            "Precision@K": sum(precisions) / len(precisions) if precisions else 0.0,
            "Recall@K": sum(recalls) / len(recalls) if recalls else 0.0,
            "nDCG@K": sum(ndcgs) / len(ndcgs) if ndcgs else 0.0,
        }

        macro_metrics = {
            "HitRate@K": sum(d["hit"] for d in per_session_results) / len(per_session_results) if per_session_results else 0.0,
            "MRR@K": sum(d["mrr"] for d in per_session_results) / len(per_session_results) if per_session_results else 0.0,
            "Precision@K": sum(d["precision"] for d in per_session_results) / len(per_session_results) if per_session_results else 0.0,
            "Recall@K": sum(d["recall"] for d in per_session_results) / len(per_session_results) if per_session_results else 0.0,
            "nDCG@K": sum(d["ndcg"] for d in per_session_results) / len(per_session_results) if per_session_results else 0.0,
        }

        return {"MicroMetrics": micro_metrics, "MacroMetrics": macro_metrics}

    def close(self):
        if self._owns_rec and self.rec:
            self.rec.close()


# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="Path to test sessions JSON")
    parser.add_argument("--top_k", type=int, default=10)
    args = parser.parse_args()

    sessions = json.load(open(args.data))
    validator = Validator(top_k=args.top_k)

    models = ["hsp", "ric"]
    results = {}

    for model_name in models:
        results[model_name] = validator.evaluate_model(sessions, model_name)

    print("\n📊 Evaluation Results (HSP vs RIC)")
    for metric_type in ["MicroMetrics", "MacroMetrics"]:
        print(f"\n---- {metric_type} ----")
        header = f"{'Metric':<12} {'HSP':>10} {'RIC':>10}"
        print(header)
        print("-" * len(header))
        for metric in ["HitRate@K", "MRR@K", "Precision@K", "Recall@K", "nDCG@K"]:
            hsp_val = results["hsp"][metric_type][metric]
            ric_val = results["ric"][metric_type][metric]
            print(f"{metric:<12} {hsp_val:10.4f} {ric_val:10.4f}")

    validator.close()
