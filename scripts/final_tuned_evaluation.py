import argparse
import csv
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.tune_hyperparams import load_sessions
from src.evaluation.validator import Validator
from src.inference.recommender import Recommender


def load_best_params(csv_path: Path, model_name: str):
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} does not exist.")

    best_row = None
    best_metric = -1.0
    best_tiebreak = -1.0

    with csv_path.open("r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["model"].lower() != model_name.lower():
                continue

            hr = float(row.get("HitRate@K", 0.0))
            mrr = float(row.get("MRR@K", 0.0))
            if hr > best_metric or (hr == best_metric and mrr > best_tiebreak):
                best_metric = hr
                best_tiebreak = mrr
                best_row = row

    if not best_row:
        print(f"No valid hyperparameters found for {model_name}, using defaults")
        return 0.33, 0.33, 0.34

    return (
        float(best_row.get("alpha", 0.33)),
        float(best_row.get("beta", 0.33)),
        float(best_row.get("gamma", 0.34)),
    )


def evaluate_with_best_params(
    model_name: str,
    sessions: list[list[int]],
    top_k: int,
    tuning_csv: Path,
    output_csv: Path,
    transition_mix: float,
):
    print(f"\nEvaluating {model_name.upper()} with best hyperparameters...")
    alpha, beta, gamma = load_best_params(tuning_csv, model_name)
    print(f"Using alpha={alpha}, beta={beta}, gamma={gamma}")

    rec = Recommender(top_k=top_k, transition_mix=transition_mix)
    rec.set_weights(alpha=alpha, beta=beta, gamma=gamma)

    validator = Validator(top_k=top_k, recommender=rec)
    results = validator.evaluate_model(sessions, model_name)
    rec.close()

    print(f"\nEvaluation Results ({model_name.upper()})")
    for metric_type in ["MicroMetrics", "MacroMetrics"]:
        print(f"\n---- {metric_type} ----")
        for metric, value in results[metric_type].items():
            print(f"{metric}: {value:.4f}")

    output_csv.parent.mkdir(exist_ok=True, parents=True)
    with output_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Metric", "Value"])
        writer.writeheader()
        for metric_type in ["MicroMetrics", "MacroMetrics"]:
            for metric, value in results[metric_type].items():
                writer.writerow({"Metric": f"{metric_type}.{metric}", "Value": value})
    print(f"\nSaved results to: {output_csv}")


def main():
    parser = argparse.ArgumentParser(description="Final tuned evaluation runner")
    parser.add_argument("--experiment", default="experiments/hsp_vs_ric.yaml")
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.experiment).open())
    data_cfg = cfg["data"]
    eval_cfg = cfg["evaluation"]
    out_cfg = cfg["outputs"]
    scoring_cfg = cfg.get("scoring", {})

    sessions = load_sessions(
        data_cfg["eval_path"],
        max_sessions=data_cfg.get("max_sessions"),
    )
    print(f"Loaded {len(sessions):,} sessions for final tuned eval.")

    tuning_csv = Path(out_cfg["tuning_table"])
    evaluate_with_best_params(
        "hsp",
        sessions=sessions,
        top_k=int(eval_cfg["top_k"]),
        tuning_csv=tuning_csv,
        output_csv=Path(out_cfg["final_hsp"]),
        transition_mix=float(scoring_cfg.get("transition_mix", 0.5)),
    )
    evaluate_with_best_params(
        "ric",
        sessions=sessions,
        top_k=int(eval_cfg["top_k"]),
        tuning_csv=tuning_csv,
        output_csv=Path(out_cfg["final_ric"]),
        transition_mix=float(scoring_cfg.get("transition_mix", 0.5)),
    )


if __name__ == "__main__":
    main()
