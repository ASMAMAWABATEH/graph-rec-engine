# scripts/final_tuned_evaluation.py

import csv
import json
from pathlib import Path
from src.evaluation.validator import Validator
from src.inference.recommender import Recommender

HYPERPARAM_CSV = Path("results/tables/hyperparam_tuning.csv")
TEST_SESSION_JSON = Path("data/test_session.json")
TOP_K = 10

def load_best_params(csv_path: Path, model_name: str):
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} does not exist.")

    best_row = None
    best_metric = -1.0

    with csv_path.open("r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["model"].lower() != model_name.lower():
                continue

            hr = float(row.get("HitRate@K", 0.0))

            # ✅ Skip useless configs
            if hr == 0.0:
                continue

            if hr > best_metric:
                best_metric = hr
                best_row = row

    # ✅ Safe fallback
    if not best_row:
        print(f"⚠️ No valid hyperparameters found for {model_name}, using defaults")
        return 0.33, 0.33, 0.34

    alpha = float(best_row.get("alpha", 0.33))
    beta = float(best_row.get("beta", 0.33))
    gamma = float(best_row.get("gamma", 0.34))
    return alpha, beta, gamma

def evaluate_with_best_params(model_name: str):
    print(f"\nEvaluating {model_name.upper()} with best hyperparameters...")

    alpha, beta, gamma = load_best_params(HYPERPARAM_CSV, model_name)
    print(f"Using α={alpha}, β={beta}, γ={gamma}")

    # Create recommender with top_k
    rec = Recommender(top_k=TOP_K)

    # Apply weights if HSP or RIC models support it
    if hasattr(rec, "set_weights"):
        rec.set_weights(alpha=alpha, beta=beta, gamma=gamma)
    else:
        # Otherwise store in recommender object directly for HSP tuning
        rec.alpha = alpha
        rec.beta = beta
        rec.gamma = gamma

    # Load test sessions
    sessions = json.load(TEST_SESSION_JSON.open())

    # Use Validator for evaluation
    validator = Validator(top_k=TOP_K)

    results = validator.evaluate_model(sessions, model_name)
    validator.close()
    rec.close()

    # Print nicely
    print(f"\n📊 Evaluation Results ({model_name.upper()})")
    for metric_type in ["MicroMetrics", "MacroMetrics"]:
        print(f"\n---- {metric_type} ----")
        for metric, value in results[metric_type].items():
            print(f"{metric}: {value:.4f}")

    # Optionally save to CSV
    output_csv = Path(f"results/tables/final_eval_{model_name.lower()}.csv")
    output_csv.parent.mkdir(exist_ok=True, parents=True)
    with output_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Metric", "Value"])
        writer.writeheader()
        for metric_type in ["MicroMetrics", "MacroMetrics"]:
            for metric, value in results[metric_type].items():
                writer.writerow({"Metric": f"{metric_type}.{metric}", "Value": value})
    print(f"\n✅ Results saved to: {output_csv}")


if __name__ == "__main__":
    models = ["hsp", "ric"]
    for model in models:
        evaluate_with_best_params(model)