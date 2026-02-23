import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import yaml


def load_metrics(csv_file: Path):
    metrics = {}
    with csv_file.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            metric_name = row["Metric"].split(".")[-1]
            metrics[metric_name] = float(row["Value"])
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Compare final HSP vs RIC evaluation outputs")
    parser.add_argument("--experiment", default="experiments/hsp_vs_ric.yaml")
    parser.add_argument("--plot", action="store_true", help="Save comparison plot as PNG")
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.experiment).open())
    out_cfg = cfg["outputs"]

    hsp_csv = Path(out_cfg["final_hsp"])
    ric_csv = Path(out_cfg["final_ric"])
    output_csv = Path(out_cfg["comparison"])
    plot_path = output_csv.with_suffix(".png")

    hsp_metrics = load_metrics(hsp_csv)
    ric_metrics = load_metrics(ric_csv)

    output_csv.parent.mkdir(exist_ok=True, parents=True)
    with output_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "HSP", "RIC"])
        for metric in hsp_metrics.keys():
            writer.writerow([metric, hsp_metrics[metric], ric_metrics.get(metric, 0.0)])

    print(f"Merged evaluation saved to {output_csv}")

    if args.plot:
        metrics_names = list(hsp_metrics.keys())
        hsp_values = [hsp_metrics[m] for m in metrics_names]
        ric_values = [ric_metrics[m] for m in metrics_names]

        x = range(len(metrics_names))
        width = 0.35

        plt.figure(figsize=(10, 5))
        plt.bar(x, hsp_values, width=width, label="HSP", alpha=0.85)
        plt.bar([i + width for i in x], ric_values, width=width, label="RIC", alpha=0.85)
        plt.xticks([i + width / 2 for i in x], metrics_names, rotation=45)
        plt.ylabel("Metric Value")
        plt.title("HSP vs RIC - Final Evaluation Comparison")
        plt.legend()
        plt.tight_layout()
        plt.savefig(plot_path, dpi=180)
        print(f"Comparison plot saved to {plot_path}")


if __name__ == "__main__":
    main()
