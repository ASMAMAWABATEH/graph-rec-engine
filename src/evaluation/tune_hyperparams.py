# src/evaluation/tune_hyperparams.py

import json
import itertools
import csv
from pathlib import Path
from tqdm import tqdm
import pandas as pd

from src.inference.recommender import Recommender
from src.evaluation.metrics import hit_rate_at_k, mrr_at_k, precision_at_k, recall_at_k, ndcg_at_k


class HyperparamTuner:
    def __init__(self, sessions, top_k=10, alphas=None, betas=None, gammas=None, transition_mix: float = 0.5):
        self.sessions = sessions
        self.top_k = top_k
        self.transition_mix = transition_mix

        # Define default ranges if not provided
        self.alphas = alphas if alphas is not None else [0.0, 0.25, 0.5, 0.75, 1.0]
        self.betas = betas if betas is not None else [0.0, 0.25, 0.5, 0.75, 1.0]
        self.gammas = gammas if gammas is not None else [0.0, 0.25, 0.5, 0.75, 1.0]

        self.rec = Recommender(top_k=top_k, transition_mix=transition_mix)

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

    def tune(self, model_name="hsp", output_csv="results/tables/hyperparam_tuning.csv", append=False):
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
        output_path = Path(output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if append and output_path.exists() else "w"
        write_header = mode == "w"
        with output_path.open(mode, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            for row in results:
                writer.writerow(row)

        print(f"\n✅ Hyperparameter tuning complete. Results saved to: {output_csv}")
        return results


def load_sessions(data_path: str, max_sessions: int | None = None):
    path = Path(data_path)
    if not path.exists():
        raise FileNotFoundError(f"{path} not found.")

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
        if "session_id" not in df.columns or "item_id" not in df.columns:
            raise ValueError("Parquet must contain session_id and item_id columns")

        sort_cols = ["session_id"]
        if "position" in df.columns:
            sort_cols.append("position")
        elif "timestamp" in df.columns:
            sort_cols.append("timestamp")

        df = df.sort_values(sort_cols)
        sessions = (
            df.groupby("session_id", sort=True)["item_id"]
            .apply(lambda s: [int(x) for x in s.tolist()])
            .tolist()
        )
    else:
        payload = json.load(path.open())
        if not isinstance(payload, list):
            raise ValueError("JSON must be a list of sessions")
        if payload and isinstance(payload[0], dict):
            grouped = {}
            for row in payload:
                sid = row.get("session_id")
                item = row.get("item_id")
                if sid is None or item is None:
                    continue
                grouped.setdefault(int(sid), []).append(int(item))
            sessions = [seq for _, seq in sorted(grouped.items())]
        else:
            sessions = [[int(x) for x in seq] for seq in payload]

    sessions = [s for s in sessions if len(s) >= 2]
    if max_sessions is not None:
        sessions = sessions[:max_sessions]
    return sessions


def parse_floats(values: str | None):
    if not values:
        return None
    return [float(v.strip()) for v in values.split(",") if v.strip()]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="Path to sessions JSON or Parquet")
    parser.add_argument("--model", choices=["hsp", "ric"], default="hsp")
    parser.add_argument("--top_k", type=int, default=10)
    parser.add_argument("--max_sessions", type=int, default=None)
    parser.add_argument("--alphas", type=str, default=None, help="Comma-separated floats")
    parser.add_argument("--betas", type=str, default=None, help="Comma-separated floats")
    parser.add_argument("--gammas", type=str, default=None, help="Comma-separated floats")
    parser.add_argument("--transition_mix", type=float, default=0.5)
    parser.add_argument("--output_csv", type=str, default="results/tables/hyperparam_tuning.csv")
    parser.add_argument("--append", action="store_true")
    args = parser.parse_args()

    sessions = load_sessions(args.data, max_sessions=args.max_sessions)

    tuner = HyperparamTuner(
        sessions=sessions,
        top_k=args.top_k,
        alphas=parse_floats(args.alphas),
        betas=parse_floats(args.betas),
        gammas=parse_floats(args.gammas),
        transition_mix=args.transition_mix,
    )
    tuner.tune(model_name=args.model, output_csv=args.output_csv, append=args.append)
