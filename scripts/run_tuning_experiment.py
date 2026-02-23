import argparse
import sys
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.tune_hyperparams import HyperparamTuner, load_sessions


def main() -> None:
    parser = argparse.ArgumentParser(description="Run tuning jobs from experiment YAML")
    parser.add_argument(
        "--experiment",
        default="experiments/hsp_vs_ric.yaml",
        help="Experiment YAML path",
    )
    args = parser.parse_args()

    cfg = yaml.safe_load(open(args.experiment))
    data_cfg = cfg["data"]
    eval_cfg = cfg["evaluation"]
    search_cfg = cfg["search"]
    output_csv = cfg["outputs"]["tuning_table"]

    sessions = load_sessions(
        data_cfg["eval_path"],
        max_sessions=data_cfg.get("max_sessions"),
    )
    print(f"Loaded {len(sessions):,} sessions for tuning.")

    models = search_cfg["models"]
    for idx, model_name in enumerate(["hsp", "ric"]):
        model_grid = models[model_name]
        tuner = HyperparamTuner(
            sessions=sessions,
            top_k=eval_cfg["top_k"],
            alphas=model_grid["alpha"],
            betas=model_grid["beta"],
            gammas=model_grid["gamma"],
        )
        tuner.tune(
            model_name=model_name,
            output_csv=output_csv,
            append=(idx > 0),
        )
        tuner.rec.close()

    print(f"Tuning complete. Combined output: {output_csv}")


if __name__ == "__main__":
    main()
