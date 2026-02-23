import pytest

from src.models.hsp_model import HSPModel


def test_hsp_score_uses_last_and_prev_items():
    model = HSPModel(
        next_edges={
            1: {3: 2},
            2: {3: 4, 4: 1},
        }
    )

    scores = model.score([1, 2], alpha=1.0, beta=0.5, gamma=0.5)

    assert scores[3] == pytest.approx(4.5)
    assert scores[4] == pytest.approx(1.0)


def test_hsp_predict_sorts_descending():
    model = HSPModel(
        next_edges={
            1: {3: 2},
            2: {3: 4, 4: 1},
        }
    )

    pred = model.predict([1, 2], top_k=2, alpha=1.0, beta=0.5, gamma=0.5)
    assert pred == [3, 4]


def test_hsp_empty_session_returns_empty():
    model = HSPModel(next_edges={1: {2: 1}})
    assert model.score([]) == {}
    assert model.predict([]) == []
