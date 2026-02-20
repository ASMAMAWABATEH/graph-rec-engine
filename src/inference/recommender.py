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
        """Graph-based session recommender orchestrator.

        Wraps HSP and RIC models backed by lightweight in-memory edge maps
        loaded from Neo4j. Also supports an optional `lookup.json` mapping
        produced by `database/build_bulk.py` so the CLI can accept either
        sequential import IDs or original item IDs transparently.
        """
        self.top_k = top_k
        self.decay = decay
        self.driver = Neo4jDriver()

        # Load lightweight in-memory edge maps from the graph for the models
        self.next_edges, self.cooccurs = self._load_graph_edges()

        # Try to load lookup mapping produced during bulk import
        self.lookup = self._load_lookup()

        # Initialize modular models using in-memory maps
        self.hsp_model = HSPModel(next_edges=self.next_edges)
        self.ric_model = RICModel(next_edges=self.cooccurs, decay=self.decay)

    def _load_graph_edges(self):
        """Read NEXT and CO_OCCURS edges from Neo4j and build dict maps.

        Returns:
            (next_edges, cooccurs): two dicts mapping src_item -> {dst_item: weight}
        """
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
            # If something goes wrong (e.g., empty DB), return empty maps
            next_edges = {}
            cooccurs = {}

        return next_edges, cooccurs

    def _load_lookup(self):
        """Load `data/neo4j_import/lookup.json` if available and build reverse map.

        Returns dict with keys:
            - item_to_seq: {original_id_str: seq_id}
            - seq_to_item: {seq_id: original_id_int}
        """
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
        """Map provided session item IDs to the original `item_id` values present in the DB.

        Mapping strategy:
        - If the item matches a key in the in-memory edge maps, keep it.
        - Else if a `lookup` exists and the value looks like a sequence id, map to original.
        - Otherwise return the item unchanged.
        """
        if not session_items:
            return []

        mapped = []
        for it in session_items:
            # already matches edge map (assume it's an original item_id)
            if it in self.next_edges or it in self.cooccurs:
                mapped.append(it)
                continue

            # try seq->original using lookup
            if self.lookup and isinstance(it, int):
                seq_map = self.lookup.get("seq_to_item", {})
                if it in seq_map:
                    mapped.append(seq_map[it])
                    continue

            # fallback: keep original
            mapped.append(it)

        return mapped

    def hsp_predict(self, session_items: List[int]) -> List[int]:
        """Predict next items using Hierarchical Sequence Probability model."""
        if not session_items:
            return global_top_k(self.top_k)

        mapped = self._map_session_items(session_items)
        return self.hsp_model.predict(mapped, top_k=self.top_k)

    def ric_predict(self, session_items: List[int]) -> List[int]:
        """Predict next items using Recurrent Item Co-occurrence model."""
        if not session_items:
            return global_top_k(self.top_k)

        mapped = self._map_session_items(session_items)

        # If there is a precomputed co-occurrence map (unlikely after bulk import), use it
        if self.cooccurs:
            scores = {}
            for idx, item in enumerate(reversed(mapped)):
                weight = self.decay ** idx
                if item in self.cooccurs:
                    for tgt, w in self.cooccurs[item].items():
                        scores[tgt] = scores.get(tgt, 0) + weight * w
        else:
            # Compute co-occurrence on the fly from Session CONTAINS relationships
            CO_QUERY = """
            MATCH (i:Item {item_id: $item})<-[:CONTAINS]-(s:Session)-[:CONTAINS]->(n:Item)
            WHERE n.item_id <> $item
            RETURN n.item_id AS item, count(*) AS w
            """

            scores = {}
            for idx, item in enumerate(reversed(mapped)):
                w_decay = self.decay ** idx
                try:
                    rows = self.driver.read_query(CO_QUERY, {"item": item})
                except Exception:
                    rows = []

                for r in rows:
                    tgt = int(r["item"])
                    w = float(r.get("w", 0))
                    scores[tgt] = scores.get(tgt, 0) + w_decay * w

        # avoid recommending items already in session
        for i in mapped:
            scores.pop(i, None)

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [item for item, _ in ranked[: self.top_k]]

    def close(self):
        """Close Neo4j driver connection."""
        if self.driver:
            self.driver.close()


# ---------------------------
# CLI / Quick test
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
