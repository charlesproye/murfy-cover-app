import unittest

from pandas import Series

from core.stats_utils import weighted_mean


class TestWeightedMean(unittest.TestCase):
    def test_basic_case(self):
        data = Series([1, 2, 3, 4])
        weights = Series([1, 2, 3, 4])
        expected = 3.0
        result = weighted_mean(data, weights)
        self.assertEqual(result, expected)

    def test_equal_weights(self):
        data = Series([10, 20, 30])
        weights = Series([1, 1, 1])
        expected = data.mean()
        result = weighted_mean(data, weights)
        self.assertEqual(result, expected)

