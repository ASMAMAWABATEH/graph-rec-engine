import pytest

from src.models.ric_model import RICModel


def test_ric_score_applies_weights_and_removes_seen_items():
    model = RICModel(
        next_edges={
            1: {3: 2, 1: 9},
            2: {3: 4, 4: 1},
        },
        decay=0.7,
    )

    scores = model.score([1, 2], alpha=1.0, beta=0.5, gamma=0.5)

    # 3 -> 4.0 from last item + 0.5*2*0.5 from previous
    assert scores[3] == pytest.approx(4.5)
    assert scores[4] == pytest.approx(1.0)
    # seen items should be removed
    assert 1 not in scores
    assert 2 not in scores


def test_ric_predict_sorts_descending():
    model = RICModel(
        next_edges={
            1: {3: 2},
            2: {3: 4, 4: 1},
        },
        decay=0.7,
    )

    pred = model.predict([1, 2], top_k=2, alpha=1.0, beta=0.5, gamma=0.5)
    assert pred == [3, 4]


def test_ric_empty_inputs_return_empty():
    model = RICModel(next_edges={}, decay=0.7)
    assert model.score([]) == {}
    assert model.predict([]) == []
