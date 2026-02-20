# src/inference/run_recommender.py
import argparse
import logging
from .recommender import Recommender
from .cold_start import global_top_k

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description="Session-Based Graph Recommender CLI (HSP / RIC with Cold-Start)"
    )
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
    session_items = args.session

    rec = Recommender(top_k=args.top_k, decay=args.decay)

    try:
        # Cold-start fallback
        if not session_items:
            recommendations = global_top_k(args.top_k)
            print(f"Session empty → using global top-{args.top_k} popular items:")
            for idx, item_id in enumerate(recommendations, 1):
                print(f"{idx}. Item {item_id}")
            return

        # Model-based recommendations
        if args.model == "hsp":
            recommendations = rec.hsp_predict(session_items)
        else:
            recommendations = rec.ric_predict(session_items)

        print(f"\nTop-{args.top_k} recommendations ({args.model.upper()}):")
        for idx, item_id in enumerate(recommendations, 1):
            print(f"{idx}. Item {item_id}")

    finally:
        rec.close()


if __name__ == "__main__":
    main()
