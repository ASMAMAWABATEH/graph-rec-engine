# scripts/compare_final_eval.py
import csv
from pathlib import Path
import matplotlib.pyplot as plt

HSP_CSV = Path("results/tables/final_eval_hsp.csv")
RIC_CSV = Path("results/tables/final_eval_ric.csv")
OUTPUT_CSV = Path("results/tables/final_eval_comparison.csv")

def load_metrics(csv_file):
    """Load metrics from a CSV into a dict"""
    metrics = {}
    with csv_file.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            metric_name = row["Metric"].split(".")[-1]  # Only keep metric, ignore Micro/Macro prefix
            metrics[metric_name] = float(row["Value"])
    return metrics

# Load metrics
hsp_metrics = load_metrics(HSP_CSV)
ric_metrics = load_metrics(RIC_CSV)

# Save merged table
OUTPUT_CSV.parent.mkdir(exist_ok=True, parents=True)
with OUTPUT_CSV.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Metric", "HSP", "RIC"])
    for metric in hsp_metrics.keys():
        writer.writerow([metric, hsp_metrics[metric], ric_metrics.get(metric, 0.0)])

print(f"✅ Merged evaluation saved to {OUTPUT_CSV}")

# Optional: bar chart
metrics_names = list(hsp_metrics.keys())
hsp_values = [hsp_metrics[m] for m in metrics_names]
ric_values = [ric_metrics[m] for m in metrics_names]

x = range(len(metrics_names))
width = 0.35

plt.figure(figsize=(10, 5))
plt.bar(x, hsp_values, width=width, label="HSP", alpha=0.8)
plt.bar([i + width for i in x], ric_values, width=width, label="RIC", alpha=0.8)
plt.xticks([i + width/2 for i in x], metrics_names, rotation=45)
plt.ylabel("Metric Value")
plt.title("HSP vs RIC - Final Evaluation Comparison")
plt.legend()
plt.tight_layout()
plt.show()