# src/evaluation/tune_hyperparams.py

import json
import itertools
import csv
from tqdm import tqdm

from src.inference.recommender import Recommender
from src.evaluation.metrics import hit_rate_at_k, mrr_at_k, precision_at_k, recall_at_k, ndcg_at_k


class HyperparamTuner:
    def __init__(self, sessions, top_k=10, alphas=None, betas=None, gammas=None):
        self.sessions = sessions
        self.top_k = top_k

        # Define default ranges if not provided
        self.alphas = alphas if alphas is not None else [0.0, 0.25, 0.5, 0.75, 1.0]
        self.betas = betas if betas is not None else [0.0, 0.25, 0.5, 0.75, 1.0]
        self.gammas = gammas if gammas is not None else [0.0, 0.25, 0.5, 0.75, 1.0]

        self.rec = Recommender(top_k=top_k)

    def evaluate_model(self, model_name):
        """Evaluate current Recommender configuration."""
        hits, mrrs, precisions, recalls, ndcgs = [], [], [], [], []

        for session in self.sessions:
            if len(session) < 2:
                continue
            input_seq = session[:-1]
            target = session[-1]

            if model_name == "hsp":
                recs = self.rec.hsp_predict(input_seq)
            else:
                recs = self.rec.ric_predict(input_seq)

            hits.append(hit_rate_at_k(recs, target))
            mrrs.append(mrr_at_k(recs, target))
            precisions.append(precision_at_k(recs, target))
            recalls.append(recall_at_k(recs, target))
            ndcgs.append(ndcg_at_k(recs, target, k=self.top_k))

        return {
            "HitRate@K": sum(hits)/len(hits) if hits else 0.0,
            "MRR@K": sum(mrrs)/len(mrrs) if mrrs else 0.0,
            "Precision@K": sum(precisions)/len(precisions) if precisions else 0.0,
            "Recall@K": sum(recalls)/len(recalls) if recalls else 0.0,
            "nDCG@K": sum(ndcgs)/len(ndcgs) if ndcgs else 0.0
        }

    def tune(self, model_name="hsp", output_csv="results/tables/hyperparam_tuning.csv"):
        results = []

        # Iterate all combinations
        for α, β, γ in tqdm(itertools.product(self.alphas, self.betas, self.gammas),
                             total=len(self.alphas)*len(self.betas)*len(self.gammas)):
            # Optional normalization to sum 1
            total = α + β + γ
            α_norm, β_norm, γ_norm = (α/total, β/total, γ/total) if total > 0 else (0.0, 0.0, 0.0)

            # Set weights in Recommender
            self.rec.set_weights(alpha=α_norm, beta=β_norm, gamma=γ_norm)

            # Evaluate
            metrics = self.evaluate_model(model_name)
            metrics.update({"alpha": α_norm, "beta": β_norm, "gamma": γ_norm, "model": model_name})
            results.append(metrics)

        # Save results to CSV
        fieldnames = ["model", "alpha", "beta", "gamma", "HitRate@K", "MRR@K", "Precision@K", "Recall@K", "nDCG@K"]
        with open(output_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)

        print(f"\n✅ Hyperparameter tuning complete. Results saved to: {output_csv}")
        return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="Path to test sessions JSON")
    parser.add_argument("--model", choices=["hsp", "ric"], default="hsp")
    parser.add_argument("--top_k", type=int, default=10)
    args = parser.parse_args()

    # Load sessions
    sessions = json.load(open(args.data))

    tuner = HyperparamTuner(sessions=sessions, top_k=args.top_k)
    tuner.tune(model_name=args.model)