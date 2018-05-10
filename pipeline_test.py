import unittest

from decimal import Decimal
from time import time

from pipeline import too_old, vwap

class TestStringMethods(unittest.TestCase):

    def test_too_old(self):
        one_hour_future = { 'timeStamp': time() + 60 * 60 * 1000 }
        now = { 'timeStamp': time() }
        three_seconds_ago = {'timeStamp': time() - 3 * 1000}
        one_hour_ago = {'timeStamp': time() - 60 * 60 * 1000}

        self.assertFalse(too_old(one_hour_future, now))
        self.assertFalse(too_old(now, now))
        self.assertFalse(too_old(three_seconds_ago, now))
        self.assertTrue(too_old(one_hour_ago, now))

    def test_vwap(self):
        instrument = [
            {
                'direction': 'buy',
                'price': 0.0477,
                'quantity': 1.4000000000000001,
            }
        ]
        self.assertEqual(Decimal('0.0477'), vwap(instrument))

        instrument = [
            {
                'direction': 'buy',
                'price': 0.05,
                'quantity': 1,
            },
            {
                'direction': 'buy',
                'price': 0.05,
                'quantity': 1,
            }
        ]
        self.assertEqual(Decimal('0.05'), vwap(instrument))

        instrument = [
            {
                'direction': 'buy',
                'price': 1,
                'quantity': 1,
            },
            {
                'direction': 'buy',
                'price': 2,
                'quantity': 1,
            }
        ]
        self.assertEqual(Decimal('1.5'), vwap(instrument))

        instrument = [
            {
                'direction': 'buy',
                'price': 1,
                'quantity': 0.5,
            },
            {
                'direction': 'buy',
                'price': 1,
                'quantity': 1,
            }
        ]
        self.assertEqual(Decimal('1.0'), vwap(instrument))

if __name__ == '__main__':
    unittest.main()
