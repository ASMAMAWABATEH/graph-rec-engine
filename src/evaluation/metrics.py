# src/evaluation/metrics.py

import math
from typing import List


def hit_rate_at_k(recommended: List[int], ground_truth: int) -> int:
    return int(ground_truth in recommended)


def mrr_at_k(recommended: List[int], ground_truth: int) -> float:
    if ground_truth in recommended:
        rank = recommended.index(ground_truth) + 1
        return 1.0 / rank
    return 0.0


def precision_at_k(recommended: List[int], ground_truth: int) -> float:
    return 1.0 / len(recommended) if ground_truth in recommended else 0.0


def recall_at_k(recommended: List[int], ground_truth: int) -> float:
    return float(ground_truth in recommended)


def ndcg_at_k(recommended: List[int], ground_truth: int, k: int = 10) -> float:
    """
    Normalized Discounted Cumulative Gain for a single ground-truth item.
    Returns 1 / log2(rank + 1) if the item is present, else 0.
    """
    if ground_truth in recommended[:k]:
        rank = recommended.index(ground_truth) + 1  # rank starts at 1
        return 1.0 / (math.log2(rank + 1))
    return 0.0