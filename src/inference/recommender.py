# src/inference/recommender.py
import json
import os
import logging
from typing import List

from database.driver import Neo4jDriver
from .cold_start import global_top_k
from ..models.hsp_model import HSPModel
from ..models.ric_model import RICModel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


class Recommender:
    def __init__(self, top_k: int = 10, decay: float = 0.7):
        """Graph-based session recommender orchestrator."""
        self.top_k = top_k
        self.decay = decay
        self.driver = Neo4jDriver()

        # Default weights for scoring
        self.alpha = 0.5  # NEXT_weight
        self.beta = 0.3   # CO_OCCURS_weight
        self.gamma = 0.2  # Recency_weight

        # Load in-memory edge maps from Neo4j
        self.next_edges, self.cooccurs = self._load_graph_edges()

        # Try to load lookup mapping
        self.lookup = self._load_lookup()

        # Initialize modular models
        self.hsp_model = HSPModel(next_edges=self.next_edges)
        self.ric_model = RICModel(next_edges=self.cooccurs, decay=self.decay)

    def set_weights(self, alpha=None, beta=None, gamma=None):
        """Update scoring weights for HSP/RIC dynamically."""
        if alpha is not None:
            self.alpha = alpha
        if beta is not None:
            self.beta = beta
        if gamma is not None:
            self.gamma = gamma

    def _load_graph_edges(self):
        """Read NEXT and CO_OCCURS edges from Neo4j and build dict maps."""
        NEXT_QUERY = """
        MATCH (i:Item)-[r:NEXT]->(n:Item)
        RETURN i.item_id AS src, n.item_id AS dst, r.weight AS w
        """

        CO_QUERY = """
        MATCH (i:Item)-[c:CONTAINS]-(n:Item)
        RETURN i.item_id AS src, n.item_id AS dst, c.weight AS w
        """

        next_edges = {}
        cooccurs = {}

        try:
            rows = self.driver.read_query(NEXT_QUERY)
            for r in rows:
                s = int(r["src"])
                d = int(r["dst"])
                w = float(r.get("w", 0))
                next_edges.setdefault(s, {})[d] = w

            rows = self.driver.read_query(CO_QUERY)
            for r in rows:
                s = int(r["src"])
                d = int(r["dst"])
                w = float(r.get("w", 0))
                cooccurs.setdefault(s, {})[d] = w

        except Exception:
            next_edges = {}
            cooccurs = {}

        return next_edges, cooccurs

    def _load_lookup(self):
        """Load lookup.json if available."""
        from pathlib import Path

        p = Path("data/neo4j_import/lookup.json")
        if not p.exists():
            return None

        try:
            raw = json.load(p.open())
            item_to_seq = raw.get("item_to_id", {})
            seq_to_item = {int(v): int(k) for k, v in item_to_seq.items()}
            return {"item_to_seq": item_to_seq, "seq_to_item": seq_to_item}
        except Exception:
            return None

    def _map_session_items(self, session_items: List[int]) -> List[int]:
        """Map session items to original item_ids using lookup if needed."""
        if not session_items:
            return []

        mapped = []
        for it in session_items:
            if it in self.next_edges or it in self.cooccurs:
                mapped.append(it)
                continue

            if self.lookup and isinstance(it, int):
                seq_map = self.lookup.get("seq_to_item", {})
                if it in seq_map:
                    mapped.append(seq_map[it])
                    continue

            mapped.append(it)

        return mapped

    def hsp_predict(self, session_items: List[int]) -> List[int]:
        """Predict next items using HSP model."""
        if not session_items:
            return global_top_k(self.top_k)

        mapped = self._map_session_items(session_items)
        return self.hsp_model.predict(
            mapped,
            top_k=self.top_k,
            alpha=self.alpha,
            beta=self.beta,
            gamma=self.gamma
        )

    def ric_predict(self, session_items: List[int]) -> List[int]:
        """Predict next items using RIC model."""
        if not session_items:
            return global_top_k(self.top_k)

        mapped = self._map_session_items(session_items)

        scores = {}
        for idx, item in enumerate(reversed(mapped)):
            w_decay = self.decay ** idx
            if item in self.cooccurs:
                for tgt, w in self.cooccurs[item].items():
                    # Use weights optionally in the future
                    scores[tgt] = scores.get(tgt, 0) + w_decay * w

        for i in mapped:
            scores.pop(i, None)

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [item for item, _ in ranked[: self.top_k]]

    def close(self):
        """Close Neo4j connection."""
        if self.driver:
            self.driver.close()


# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Session-Based Graph Recommender CLI")
    parser.add_argument(
        "--session", nargs="+", type=int, required=True,
        help="List of item IDs in the current session (ordered)"
    )
    parser.add_argument(
        "--model", choices=["hsp", "ric"], default="hsp",
        help="Select recommendation algorithm"
    )
    parser.add_argument(
        "--top_k", type=int, default=10,
        help="Number of items to recommend"
    )
    parser.add_argument(
        "--decay", type=float, default=0.7,
        help="Temporal decay factor (RIC only)"
    )

    args = parser.parse_args()

    rec = Recommender(top_k=args.top_k, decay=args.decay)

    try:
        if args.model == "hsp":
            recommendations = rec.hsp_predict(args.session)
        else:
            recommendations = rec.ric_predict(args.session)

        print(f"Top-{args.top_k} recommendations ({args.model.upper()}):")
        for idx, item_id in enumerate(recommendations, 1):
            print(f"{idx}. Item {item_id}")

    finally:
        rec.close()