import pytest
from utils.helpers import normalize_coords


def test_understat_midpoint():
    x, y = normalize_coords(0.5, 0.5, "understat")
    assert x == 52.5
    assert y == 34.0


def test_understat_origin():
    x, y = normalize_coords(0.0, 0.0, "understat")
    assert x == 0.0
    assert y == 0.0


def test_understat_max():
    x, y = normalize_coords(1.0, 1.0, "understat")
    assert x == 105.0
    assert y == 68.0


def test_statsbomb_max():
    x, y = normalize_coords(120.0, 80.0, "statsbomb")
    assert abs(x - 105.0) < 0.001
    assert abs(y - 68.0) < 0.001


def test_statsbomb_midpoint():
    x, y = normalize_coords(60.0, 40.0, "statsbomb")
    assert abs(x - 52.5) < 0.001
    assert abs(y - 34.0) < 0.001


def test_sofascore_max():
    x, y = normalize_coords(100.0, 100.0, "sofascore")
    assert abs(x - 105.0) < 0.001
    assert abs(y - 68.0) < 0.001


def test_sofascore_midpoint():
    x, y = normalize_coords(50.0, 50.0, "sofascore")
    assert abs(x - 52.5) < 0.001
    assert abs(y - 34.0) < 0.001


def test_whoscored_midpoint():
    x, y = normalize_coords(50.0, 50.0, "whoscored")
    assert abs(x - 52.5) < 0.001
    assert abs(y - 34.0) < 0.001


def test_unknown_source_raises():
    with pytest.raises(ValueError, match="Unknown source"):
        normalize_coords(50.0, 50.0, "unknown_source")
