import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from database.build_bulk import build_bulk_from_rows
from src.evaluation.validator import Validator
from src.inference.recommender import Recommender
from src.preprocessing.sessionizer import Sessionizer
from src.preprocessing.split import SessionSplitter
from src.utils.logger import get_logger


logger = get_logger(__name__)
DEFAULT_CONFIG_PATH = Path("configs/pipeline.yaml")
BULK_REQUIRED_FILES = (
    "items.csv",
    "sessions.csv",
    "contains_typed.csv",
    "next_typed.csv",
    "cooccurs_typed.csv",
    "lookup.json",
)


def _load_yaml_cfg(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open() as f:
        return yaml.safe_load(f) or {}


def _bootstrap_config_path() -> Path:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    args, _ = parser.parse_known_args()
    return args.config


def _load_sessions_json(payload: Any) -> list[list[int]]:
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        return [[int(x) for x in sess] for sess in payload]

    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        by_session: dict[int, list[int]] = {}
        for row in payload:
            sid = row.get("session_id")
            item = row.get("item_id")
            nxt = row.get("next_item_id")
            if sid is None or item is None:
                continue
            sid = int(sid)
            by_session.setdefault(sid, []).append(int(item))
            if nxt is not None:
                by_session[sid].append(int(nxt))
        return [items for _, items in sorted(by_session.items()) if items]

    raise ValueError("Unsupported JSON session format")


def _sort_columns(df: pd.DataFrame) -> list[str]:
    sort_cols = ["session_id"]
    if "position" in df.columns:
        sort_cols.append("position")
    elif "timestamp" in df.columns:
        sort_cols.append("timestamp")
    return sort_cols


def load_sessions(data_path: Path, max_sessions: int | None = None) -> list[list[int]]:
    if not data_path.exists():
        raise FileNotFoundError(f"Evaluation data not found: {data_path}")

    if data_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(data_path)
        if "session_id" not in df.columns or "item_id" not in df.columns:
            raise ValueError("Parquet must contain session_id and item_id columns")
        df = df.sort_values(_sort_columns(df))
        sessions = (
            df.groupby("session_id", sort=True)["item_id"]
            .apply(lambda s: [int(x) for x in s.tolist()])
            .tolist()
        )
    elif data_path.suffix.lower() == ".json":
        with data_path.open() as f:
            payload = json.load(f)
        sessions = _load_sessions_json(payload)
    else:
        raise ValueError(f"Unsupported data format: {data_path.suffix}")

    sessions = [s for s in sessions if len(s) >= 2]
    if max_sessions is not None:
        sessions = sessions[:max_sessions]
    return sessions


def resolve_default_data_path() -> Path:
    parquet = Path("data/processed/test_sessions.parquet")
    if parquet.exists():
        return parquet
    return Path("data/test_session.json")


def _build_rows_from_session_df(df: pd.DataFrame) -> list[dict[str, int | None]]:
    if "session_id" not in df.columns or "item_id" not in df.columns:
        raise ValueError("Input frame must contain session_id and item_id")

    rows: list[dict[str, int | None]] = []
    ordered = df.sort_values(_sort_columns(df))
    for session_id, group in ordered.groupby("session_id", sort=True):
        items = [int(x) for x in group["item_id"].tolist()]
        for idx, item_id in enumerate(items):
            nxt = items[idx + 1] if idx + 1 < len(items) else None
            rows.append({"session_id": int(session_id), "item_id": item_id, "next_item_id": nxt})
    return rows


def _pick_best_model(metrics: dict[str, Any]) -> str:
    def score(model_name: str) -> tuple[float, float]:
        model = metrics.get(model_name, {})
        micro = model.get("MicroMetrics", {})
        return float(micro.get("HitRate@K", 0.0)), float(micro.get("MRR@K", 0.0))

    return max(("hsp", "ric"), key=score)


def _bulk_files_ready(out_dir: Path) -> bool:
    return all((out_dir / name).exists() for name in BULK_REQUIRED_FILES)


def _load_bulk_stats(out_dir: Path) -> dict[str, int]:
    stats_path = out_dir / "bulk_stats.json"
    if stats_path.exists():
        with stats_path.open() as f:
            return json.load(f)

    def tsv_rows(path: Path) -> int:
        if not path.exists():
            return 0
        with path.open() as f:
            return max(sum(1 for _ in f) - 1, 0)

    return {
        "items": tsv_rows(out_dir / "items.csv"),
        "sessions": tsv_rows(out_dir / "sessions.csv"),
        "next_edges": tsv_rows(out_dir / "next_typed.csv"),
        "contains_edges": tsv_rows(out_dir / "contains_typed.csv"),
        "cooccurs_edges": tsv_rows(out_dir / "cooccurs_typed.csv"),
    }


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(payload, f, indent=2)


def run_eval(args: argparse.Namespace) -> dict[str, Any]:
    sessions = load_sessions(args.data, max_sessions=args.max_sessions)
    if not sessions:
        raise ValueError(f"No valid sessions found in {args.data}")

    rec = Recommender(
        top_k=args.top_k,
        decay=args.decay,
        edge_time_decay_lambda=args.edge_time_decay,
        transition_mix=args.transition_mix,
    )
    try:
        rec.set_weights(alpha=args.alpha, beta=args.beta, gamma=args.gamma)
        validator = Validator(top_k=args.top_k, recommender=rec)
        try:
            results = validator.evaluate_model(sessions, args.model)
        finally:
            validator.close()
    finally:
        rec.close()

    payload = {
        "model": args.model,
        "top_k": args.top_k,
        "transition_mix": args.transition_mix,
        "results": results,
    }
    logger.info("Evaluation complete for model=%s top_k=%s", args.model, args.top_k)
    print(json.dumps(payload, indent=2))

    if args.output:
        _save_json(args.output, results)
        logger.info("Saved eval results to %s", args.output)

    return payload


def run_full_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    run_started = datetime.now(timezone.utc)

    processed_path = args.processed
    train_path = args.split_dir / "train_sessions.parquet"
    test_path = args.split_dir / "test_sessions.parquet"

    logger.info("[1/4] Preprocess data")
    if args.reuse_intermediate and processed_path.exists() and train_path.exists() and test_path.exists():
        logger.info("Reusing existing processed/split artifacts from %s", args.split_dir)
        processed_df = pd.read_parquet(processed_path)
        train_df = pd.read_parquet(train_path)
        test_df = pd.read_parquet(test_path)
    else:
        sessionizer = Sessionizer(
            min_session_length=args.min_session_length,
            min_item_freq=args.min_item_freq,
            chunksize=args.chunksize,
            max_chunks=args.max_chunks,
        )
        processed_df = sessionizer.run_pipeline(str(args.raw), str(processed_path))

        splitter = SessionSplitter(test_ratio=args.test_ratio)
        train_df, test_df = splitter.temporal_split(processed_df)
        train_path, test_path = splitter.save_split(train_df, test_df, str(args.split_dir))

    if processed_df.empty:
        raise ValueError("Preprocess output is empty.")
    if train_df.empty or test_df.empty:
        raise ValueError("Train/test split produced an empty partition.")
    logger.info(
        "Preprocess sanity: events=%s train_sessions=%s test_sessions=%s",
        len(processed_df),
        train_df["session_id"].nunique(),
        test_df["session_id"].nunique(),
    )

    logger.info("[2/4] Build graph in Neo4j")
    if args.reuse_intermediate and _bulk_files_ready(args.bulk_dir):
        logger.info("Reusing existing bulk files from %s", args.bulk_dir)
        bulk_stats = _load_bulk_stats(args.bulk_dir)
    else:
        batch_rows = _build_rows_from_session_df(train_df)
        bulk_stats = build_bulk_from_rows(batch_rows, args.bulk_dir)
        _save_json(args.bulk_dir / "bulk_stats.json", bulk_stats)
        logger.info("Created fresh bulk files in %s", args.bulk_dir)

    if bulk_stats["next_edges"] == 0 or bulk_stats["cooccurs_edges"] == 0:
        raise ValueError("Bulk graph tables are empty (NEXT/CO_OCCURS).")
    logger.info(
        "Bulk sanity: items=%s sessions=%s next=%s cooccurs=%s",
        bulk_stats["items"],
        bulk_stats["sessions"],
        bulk_stats["next_edges"],
        bulk_stats["cooccurs_edges"],
    )

    if args.skip_graph_load:
        logger.info("Skipping graph load (--skip-graph-load enabled)")
    else:
        from database.load_graph import DEFAULT_CYPHER_FILES, run_cypher_files, stage_import_files

        if args.neo4j_import_dir is not None:
            stage_import_files(args.bulk_dir, args.neo4j_import_dir)
        else:
            logger.info("No --neo4j-import-dir provided; assuming Neo4j can read %s", args.bulk_dir)

        cypher_files = DEFAULT_CYPHER_FILES if not args.skip_cooccurs else DEFAULT_CYPHER_FILES[:-1]
        run_cypher_files(cypher_files)

    logger.info("[3/4] Train HSP and RIC models")
    rec = Recommender(
        top_k=args.top_k,
        decay=args.decay,
        edge_time_decay_lambda=args.edge_time_decay,
        transition_mix=args.transition_mix,
    )
    try:
        rec.set_weights(alpha=args.alpha, beta=args.beta, gamma=args.gamma)
        model_state = {
            "next_sources": len(rec.next_edges),
            "cooccurs_sources": len(rec.cooccurs),
            "weights": {"alpha": rec.alpha, "beta": rec.beta, "gamma": rec.gamma},
            "edge_time_decay_lambda": rec.edge_time_decay_lambda,
            "transition_mix": rec.transition_mix,
        }

        logger.info("[4/4] Evaluate and save metrics")
        eval_sessions = load_sessions(test_path, max_sessions=args.max_sessions)
        if not eval_sessions:
            raise ValueError(f"No evaluation sessions found in {test_path}")

        validator = Validator(top_k=args.top_k, recommender=rec)
        try:
            subset_metrics: dict[str, Any] | None = None
            if args.subset_validate_sessions > 0:
                subset_n = min(args.subset_validate_sessions, len(eval_sessions))
                logger.info("Running subset validation on %s sessions", subset_n)
                subset_slice = eval_sessions[:subset_n]
                subset_metrics = {
                    "sessions": subset_n,
                    "hsp": validator.evaluate_model(subset_slice, "hsp"),
                    "ric": validator.evaluate_model(subset_slice, "ric"),
                }

            metrics = {
                "hsp": validator.evaluate_model(eval_sessions, "hsp"),
                "ric": validator.evaluate_model(eval_sessions, "ric"),
            }
        finally:
            validator.close()
    finally:
        rec.close()

    best_model = _pick_best_model(metrics)
    output_path = args.output or Path("results/metrics/pipeline_metrics.json")

    payload = {
        "run_started_utc": run_started.isoformat(),
        "run_finished_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": str(args.config),
        "paths": {
            "raw": str(args.raw),
            "processed": str(processed_path),
            "train": str(train_path),
            "test": str(test_path),
            "neo4j_bulk_dir": str(args.bulk_dir),
        },
        "counts": {
            "processed_events": int(len(processed_df)),
            "train_events": int(len(train_df)),
            "test_events": int(len(test_df)),
            "train_sessions": int(train_df["session_id"].nunique()),
            "test_sessions": int(test_df["session_id"].nunique()),
            "evaluated_sessions": int(len(eval_sessions)),
        },
        "bulk_stats": bulk_stats,
        "model_state": model_state,
        "subset_validation": subset_metrics,
        "top_k": args.top_k,
        "metrics": metrics,
        "best_model_by_micro_hit_rate": best_model,
    }
    _save_json(output_path, payload)
    _save_json(args.intermediate_state, {"bulk_stats": bulk_stats, "paths": payload["paths"]})

    logger.info("Saved pipeline metrics to %s", output_path)
    logger.info("Saved intermediate state to %s", args.intermediate_state)
    print(json.dumps(payload, indent=2))
    return payload


def build_parser(config_path: Path) -> argparse.ArgumentParser:
    cfg = _load_yaml_cfg(config_path)
    preprocess_cfg = _load_yaml_cfg(Path("configs/preprocessing.yaml"))
    parser = argparse.ArgumentParser(description="Graph-SBR pipeline entrypoint")
    parser.add_argument("--config", type=Path, default=config_path)
    parser.add_argument("--mode", choices=["full", "eval"], default=cfg.get("mode", "full"))

    parser.add_argument("--model", choices=["hsp", "ric"], default=cfg.get("model"))
    parser.add_argument("--top_k", type=int, default=int(cfg.get("top_k", 10)))
    parser.add_argument("--data", type=Path, default=Path(cfg.get("data", resolve_default_data_path())))
    parser.add_argument("--output", type=Path, default=Path(cfg["output"]) if cfg.get("output") else None)
    parser.add_argument("--max_sessions", type=int, default=int(cfg.get("max_sessions", 5000)))

    parser.add_argument("--raw", type=Path, default=Path(cfg.get("raw", "data/raw/yoochoose-clicks.dat")))
    parser.add_argument(
        "--processed",
        type=Path,
        default=Path(cfg.get("processed", "data/processed/yoochoose_sessions.parquet")),
    )
    parser.add_argument("--split_dir", type=Path, default=Path(cfg.get("split_dir", "data/processed")))
    parser.add_argument("--bulk_dir", type=Path, default=Path(cfg.get("bulk_dir", "data/neo4j_import")))
    parser.add_argument(
        "--neo4j-import-dir",
        type=Path,
        default=Path(cfg["neo4j_import_dir"]) if cfg.get("neo4j_import_dir") else None,
    )
    parser.add_argument("--skip-cooccurs", action="store_true", default=bool(cfg.get("skip_cooccurs", False)))
    parser.add_argument("--skip-graph-load", action="store_true", default=bool(cfg.get("skip_graph_load", False)))
    parser.add_argument("--reuse-intermediate", action=argparse.BooleanOptionalAction, default=bool(cfg.get("reuse_intermediate", True)))
    parser.add_argument(
        "--intermediate-state",
        type=Path,
        default=Path(cfg.get("intermediate_state", "results/intermediate/pipeline_state.json")),
    )

    parser.add_argument(
        "--min_session_length",
        type=int,
        default=int(cfg.get("min_session_length", preprocess_cfg.get("min_session_length", 2))),
    )
    parser.add_argument(
        "--min_item_freq",
        type=int,
        default=int(cfg.get("min_item_freq", preprocess_cfg.get("min_item_freq", 5))),
    )
    parser.add_argument(
        "--test_ratio",
        type=float,
        default=float(cfg.get("test_ratio", preprocess_cfg.get("test_ratio", 0.2))),
    )
    parser.add_argument("--chunksize", type=int, default=int(cfg.get("chunksize", 500_000)))
    parser.add_argument("--max_chunks", type=int, default=cfg.get("max_chunks"))
    parser.add_argument("--subset_validate_sessions", type=int, default=int(cfg.get("subset_validate_sessions", 200)))

    parser.add_argument("--decay", type=float, default=float(cfg.get("decay", 0.7)))
    parser.add_argument("--alpha", type=float, default=cfg.get("alpha"))
    parser.add_argument("--beta", type=float, default=cfg.get("beta"))
    parser.add_argument("--gamma", type=float, default=cfg.get("gamma"))
    parser.add_argument("--edge_time_decay", type=float, default=float(cfg.get("edge_time_decay", 0.0)))
    parser.add_argument("--transition_mix", type=float, default=float(cfg.get("transition_mix", 0.5)))
    return parser


def main() -> None:
    config_path = _bootstrap_config_path()
    parser = build_parser(config_path)
    args = parser.parse_args()

    if args.mode == "eval":
        if args.model is None:
            parser.error("--model is required when --mode eval")
        run_eval(args)
        return

    run_full_pipeline(args)


if __name__ == "__main__":
    main()
