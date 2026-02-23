import argparse
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.driver import Neo4jDriver
from src.evaluation.tune_hyperparams import load_sessions
from src.inference.recommender import Recommender


def load_processed_df(data_dir: Path) -> pd.DataFrame:
    train_path = data_dir / "train_sessions.parquet"
    test_path = data_dir / "test_sessions.parquet"
    full_path = data_dir / "yoochoose_sessions.parquet"

    if train_path.exists() and test_path.exists():
        train_df = pd.read_parquet(train_path)
        train_df["split"] = "train"
        test_df = pd.read_parquet(test_path)
        test_df["split"] = "test"
        return pd.concat([train_df, test_df], ignore_index=True)
    if full_path.exists():
        df = pd.read_parquet(full_path)
        df["split"] = "full"
        return df
    raise FileNotFoundError("No processed parquet files found in data/processed")


def generate_eda_figures(df: pd.DataFrame, out_dir: Path) -> None:
    session_lengths = df.groupby("session_id").size().rename("length")
    item_pop = df.groupby("item_id").size().sort_values(ascending=False)

    plt.figure(figsize=(10, 4))
    plt.hist(session_lengths, bins=50)
    plt.title("Session Length Distribution")
    plt.xlabel("Session length (#items)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(out_dir / "eda_session_length_hist.png", dpi=180)
    plt.close()

    plt.figure(figsize=(10, 4))
    plt.hist(session_lengths, bins=50, log=True)
    plt.title("Session Length Distribution (log-y)")
    plt.xlabel("Session length (#items)")
    plt.ylabel("Count (log scale)")
    plt.tight_layout()
    plt.savefig(out_dir / "eda_session_length_hist_log.png", dpi=180)
    plt.close()

    plt.figure(figsize=(12, 4))
    item_pop.head(20).plot(kind="bar")
    plt.title("Top-20 Most Popular Items")
    plt.xlabel("item_id")
    plt.ylabel("interaction count")
    plt.tight_layout()
    plt.savefig(out_dir / "eda_top20_items.png", dpi=180)
    plt.close()

    plt.figure(figsize=(8, 4))
    item_pop.plot(kind="hist", bins=60, log=True)
    plt.title("Item Popularity Distribution")
    plt.xlabel("interaction count per item")
    plt.ylabel("item frequency (log scale)")
    plt.tight_layout()
    plt.savefig(out_dir / "eda_item_popularity_hist_log.png", dpi=180)
    plt.close()

    if "split" in df.columns and set(df["split"].unique()) >= {"train", "test"}:
        train_lens = df[df["split"] == "train"].groupby("session_id").size()
        test_lens = df[df["split"] == "test"].groupby("session_id").size()
        plt.figure(figsize=(10, 4))
        plt.hist(train_lens, bins=40, alpha=0.6, label="train")
        plt.hist(test_lens, bins=40, alpha=0.6, label="test")
        plt.title("Train vs Test Session Lengths")
        plt.xlabel("session length")
        plt.ylabel("count")
        plt.legend()
        plt.tight_layout()
        plt.savefig(out_dir / "eda_train_vs_test_session_lengths.png", dpi=180)
        plt.close()


def fetch_top_next_transitions(limit: int):
    query = """
    MATCH (src:Item)-[r:NEXT]->(dst:Item)
    RETURN src.item_id AS src, dst.item_id AS dst, r.weight AS w
    ORDER BY w DESC
    LIMIT $limit
    """
    d = Neo4jDriver()
    try:
        return d.read_query(query, {"limit": int(limit)})
    finally:
        d.close()


def generate_transition_figure(rows, out_dir: Path) -> None:
    if not rows:
        return
    nodes = sorted({int(e["src"]) for e in rows} | {int(e["dst"]) for e in rows})
    weights = [float(e["w"]) for e in rows]
    w_min, w_max = min(weights), max(weights)

    coords = {}
    n = len(nodes)
    for i, node in enumerate(nodes):
        angle = 2 * math.pi * i / max(n, 1)
        coords[node] = (math.cos(angle), math.sin(angle))

    plt.figure(figsize=(10, 10))
    for e in rows:
        s, t, w = int(e["src"]), int(e["dst"]), float(e["w"])
        x1, y1 = coords[s]
        x2, y2 = coords[t]
        alpha = 0.2 if w_max == w_min else 0.2 + 0.8 * ((w - w_min) / (w_max - w_min))
        lw = 0.5 if w_max == w_min else 0.5 + 3.5 * ((w - w_min) / (w_max - w_min))
        plt.plot([x1, x2], [y1, y2], alpha=alpha, linewidth=lw)

    xs = [coords[node][0] for node in nodes]
    ys = [coords[node][1] for node in nodes]
    plt.scatter(xs, ys, s=90)
    for node, (x, y) in coords.items():
        plt.text(x, y, str(node), fontsize=8, ha="center", va="center")

    plt.title("Top NEXT Transitions (Neo4j)")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(out_dir / "graph_top_next_transitions.png", dpi=180)
    plt.close()


def generate_recommendation_trace(session_data: Path, out_dir: Path) -> None:
    sessions = load_sessions(str(session_data), max_sessions=1)
    if not sessions:
        raise ValueError("No sessions available for recommendation trace")

    example_full = sessions[0]
    example_input = example_full[:-1][-5:]
    example_target = example_full[-1]

    rec = Recommender(top_k=10)
    try:
        hsp_recs = rec.hsp_predict(example_input)
        ric_recs = rec.ric_predict(example_input)
        last_item = example_input[-1]
        hsp_reason = sorted(
            rec.next_edges.get(last_item, {}).items(),
            key=lambda x: x[1],
            reverse=True,
        )[:10]
        ric_reason = sorted(
            rec.cooccurs.get(last_item, {}).items(),
            key=lambda x: x[1],
            reverse=True,
        )[:10]
    finally:
        rec.close()

    trace_path = out_dir / "recommendation_trace.txt"
    with trace_path.open("w") as f:
        f.write(f"example_input={example_input}\n")
        f.write(f"ground_truth={example_target}\n")
        f.write(f"hsp_top10={hsp_recs}\n")
        f.write(f"ric_top10={ric_recs}\n")
        f.write(f"hsp_signal_from_last_item_{last_item}={hsp_reason}\n")
        f.write(f"ric_signal_from_last_item_{last_item}={ric_reason}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate notebook visualization smoke artifacts")
    parser.add_argument("--output_dir", type=Path, default=Path("results/figures"))
    parser.add_argument("--processed_dir", type=Path, default=Path("data/processed"))
    parser.add_argument("--session_data", type=Path, default=Path("data/processed/test_sessions.parquet"))
    parser.add_argument("--transition_limit", type=int, default=40)
    args = parser.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_processed_df(args.processed_dir)
    print(
        f"Loaded processed data: rows={len(df):,}, "
        f"sessions={df['session_id'].nunique():,}, items={df['item_id'].nunique():,}"
    )

    generate_eda_figures(df, out_dir)
    transitions = fetch_top_next_transitions(args.transition_limit)
    print(f"Loaded top transitions: {len(transitions)}")
    generate_transition_figure(transitions, out_dir)
    generate_recommendation_trace(args.session_data, out_dir)

    print(f"Visualization artifacts written to {out_dir}")


if __name__ == "__main__":
    main()
