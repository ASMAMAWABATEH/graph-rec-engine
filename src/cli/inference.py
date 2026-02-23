import argparse
import json
import time
from math import ceil
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.inference.cold_start import global_top_k
from src.inference.recommender import Recommender
from src.utils.logger import get_logger


logger = get_logger(__name__)
DEFAULT_CONFIG_PATH = Path("configs/inference.yaml")


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


def _normalize_session(items: list[Any]) -> list[int]:
    out: list[int] = []
    for item in items:
        if item is None:
            continue
        out.append(int(item))
    return out


def _sessions_from_dataframe(df: pd.DataFrame) -> list[tuple[str, list[int]]]:
    if "session_id" not in df.columns or "item_id" not in df.columns:
        raise ValueError("CSV/Parquet batch input must contain session_id and item_id columns")

    sort_cols = ["session_id"]
    if "position" in df.columns:
        sort_cols.append("position")
    elif "timestamp" in df.columns:
        sort_cols.append("timestamp")
    df = df.sort_values(sort_cols)

    sessions: list[tuple[str, list[int]]] = []
    grouped = df.groupby("session_id", sort=True)["item_id"].apply(list)
    for sid, items in grouped.items():
        sessions.append((str(sid), _normalize_session(items)))
    return sessions


def _sessions_from_json_payload(payload: Any) -> list[tuple[str, list[int]]]:
    if isinstance(payload, list):
        if not payload:
            return []

        if isinstance(payload[0], (int, str)):
            return [("0", _normalize_session(payload))]

        if isinstance(payload[0], list):
            return [(str(i), _normalize_session(sess)) for i, sess in enumerate(payload)]

        if isinstance(payload[0], dict):
            if "session_id" in payload[0] and "item_id" in payload[0]:
                by_session: dict[str, list[int]] = {}
                for row in payload:
                    sid = row.get("session_id")
                    item_id = row.get("item_id")
                    if sid is None or item_id is None:
                        continue
                    sid_key = str(sid)
                    by_session.setdefault(sid_key, []).append(int(item_id))
                return [(sid, items) for sid, items in sorted(by_session.items(), key=lambda x: x[0])]

            sessions: list[tuple[str, list[int]]] = []
            for idx, row in enumerate(payload):
                sid = str(row.get("session_id", idx))
                if "items" in row and isinstance(row["items"], list):
                    sessions.append((sid, _normalize_session(row["items"])))
                    continue
                if "session" in row and isinstance(row["session"], list):
                    sessions.append((sid, _normalize_session(row["session"])))
                    continue
                sessions.append((sid, []))
            return sessions

    if isinstance(payload, dict):
        if "sessions" in payload and isinstance(payload["sessions"], list):
            return _sessions_from_json_payload(payload["sessions"])
        if "items" in payload and isinstance(payload["items"], list):
            return [("0", _normalize_session(payload["items"]))]

    raise ValueError("Unsupported JSON format for inference input")


def load_batch_sessions(path: Path) -> list[tuple[str, list[int]]]:
    if not path.exists():
        raise FileNotFoundError(f"Inference input not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".json":
        with path.open() as f:
            payload = json.load(f)
        return _sessions_from_json_payload(payload)

    if suffix in {".csv", ".tsv"}:
        sep = "\t" if suffix == ".tsv" else ","
        df = pd.read_csv(path, sep=sep)
        return _sessions_from_dataframe(df)

    if suffix == ".parquet":
        df = pd.read_parquet(path)
        return _sessions_from_dataframe(df)

    raise ValueError(f"Unsupported input format: {suffix}")


def _predict_with_cold_start(rec: Recommender, model: str, session: list[int], top_k: int) -> tuple[list[int], bool]:
    if not session:
        return global_top_k(top_k), True

    if model == "hsp":
        recs = rec.hsp_predict(session)
    else:
        recs = rec.ric_predict(session)

    if not recs:
        return global_top_k(top_k), True
    return recs[:top_k], False


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, ceil(len(ordered) * pct) - 1))
    return ordered[idx]


def build_parser(config_path: Path) -> argparse.ArgumentParser:
    cfg = _load_yaml_cfg(config_path)
    parser = argparse.ArgumentParser(description="Inference runner for Graph-SBR (single or batch sessions)")
    parser.add_argument("--config", type=Path, default=config_path)
    parser.add_argument("--model", choices=["hsp", "ric", "both"], default=cfg.get("model", "both"))
    parser.add_argument("--top_k", type=int, default=int(cfg.get("top_k", 10)))
    parser.add_argument("--decay", type=float, default=float(cfg.get("decay", 0.7)))
    parser.add_argument("--alpha", type=float, default=cfg.get("alpha"))
    parser.add_argument("--beta", type=float, default=cfg.get("beta"))
    parser.add_argument("--gamma", type=float, default=cfg.get("gamma"))
    parser.add_argument("--edge_time_decay", type=float, default=float(cfg.get("edge_time_decay", 0.0)))
    parser.add_argument("--session", nargs="*", type=int, default=None, help="Single session as item IDs")
    parser.add_argument("--input", type=Path, default=Path(cfg["input"]) if cfg.get("input") else None)
    parser.add_argument("--output", type=Path, default=Path(cfg["output"]) if cfg.get("output") else None)
    return parser


def main() -> None:
    config_path = _bootstrap_config_path()
    parser = build_parser(config_path)
    args = parser.parse_args()

    if args.session is not None and args.input is not None:
        raise ValueError("Use either --session or --input, not both")

    if args.input is not None:
        sessions = load_batch_sessions(args.input)
    elif args.session is not None:
        sessions = [("0", _normalize_session(args.session))]
    else:
        sessions = [("0", [])]

    rec = Recommender(
        top_k=args.top_k,
        decay=args.decay,
        edge_time_decay_lambda=args.edge_time_decay,
    )
    rec.set_weights(alpha=args.alpha, beta=args.beta, gamma=args.gamma)

    try:
        output_rows = []
        total_ms = []
        hsp_ms = []
        ric_ms = []

        for session_id, items in sessions:
            row_started = time.perf_counter()
            if args.model == "both":
                hsp_started = time.perf_counter()
                hsp_recs, hsp_cold = _predict_with_cold_start(rec, "hsp", items, args.top_k)
                hsp_elapsed = (time.perf_counter() - hsp_started) * 1000.0
                hsp_ms.append(hsp_elapsed)

                ric_started = time.perf_counter()
                ric_recs, ric_cold = _predict_with_cold_start(rec, "ric", items, args.top_k)
                ric_elapsed = (time.perf_counter() - ric_started) * 1000.0
                ric_ms.append(ric_elapsed)

                row = {
                    "session_id": session_id,
                    "input_session": items,
                    "recommendations": {"hsp": hsp_recs, "ric": ric_recs},
                    "cold_start_used": {"hsp": hsp_cold, "ric": ric_cold},
                    "latency_ms": {
                        "hsp": round(hsp_elapsed, 3),
                        "ric": round(ric_elapsed, 3),
                    },
                }
            else:
                started = time.perf_counter()
                recs, cold = _predict_with_cold_start(rec, args.model, items, args.top_k)
                elapsed = (time.perf_counter() - started) * 1000.0
                if args.model == "hsp":
                    hsp_ms.append(elapsed)
                else:
                    ric_ms.append(elapsed)
                row = {
                    "session_id": session_id,
                    "input_session": items,
                    "model": args.model,
                    "recommendations": recs,
                    "cold_start_used": cold,
                    "latency_ms": round(elapsed, 3),
                }

            row_total = (time.perf_counter() - row_started) * 1000.0
            total_ms.append(row_total)
            row["total_latency_ms"] = round(row_total, 3)
            output_rows.append(row)
    finally:
        rec.close()

    latency_summary = {
        "avg_total_ms": round(sum(total_ms) / len(total_ms), 3) if total_ms else 0.0,
        "p50_total_ms": round(_percentile(total_ms, 0.50), 3) if total_ms else 0.0,
        "p95_total_ms": round(_percentile(total_ms, 0.95), 3) if total_ms else 0.0,
    }
    if hsp_ms:
        latency_summary["avg_hsp_ms"] = round(sum(hsp_ms) / len(hsp_ms), 3)
    if ric_ms:
        latency_summary["avg_ric_ms"] = round(sum(ric_ms) / len(ric_ms), 3)

    if args.session is not None and len(output_rows) == 1:
        to_print: Any = output_rows[0]
    else:
        to_print = {
            "config_path": str(args.config),
            "batch_size": len(output_rows),
            "top_k": args.top_k,
            "model": args.model,
            "latency_summary_ms": latency_summary,
            "results": output_rows,
        }

    logger.info(
        "Inference completed: sessions=%s model=%s avg_total_ms=%s",
        len(output_rows),
        args.model,
        latency_summary["avg_total_ms"],
    )
    print(json.dumps(to_print, indent=2))

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w") as f:
            json.dump(to_print, f, indent=2)
        logger.info("Saved inference output to %s", args.output)


if __name__ == "__main__":
    main()
