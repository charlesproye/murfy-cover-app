from pandas import Series

from core.stats_utils import weighted_mean


def test_basic_case():
    data = Series([1, 2, 3, 4])
    weights = Series([1, 2, 3, 4])
    expected = 3.0
    result = weighted_mean(data, weights)
    assert result == expected


def test_equal_weights():
    data = Series([10, 20, 30])
    weights = Series([1, 1, 1])
    expected = data.mean()
    result = weighted_mean(data, weights)
    assert result == expected
