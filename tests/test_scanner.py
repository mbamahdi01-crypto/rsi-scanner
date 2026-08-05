import unittest
from unittest.mock import patch

import pandas as pd

import scanner


class ScannerTests(unittest.TestCase):
    def test_volume_filter_fails_when_volume_is_missing(self):
        df = pd.DataFrame({"Close": [10.0] * 25})
        ratio, passed = scanner._volume_at_break(df, 24)
        self.assertIsNone(ratio)
        self.assertFalse(passed)

    def test_backtest_records_fresh_signal_once(self):
        df = pd.DataFrame({"Close": [100.0] * 40})

        def bullish(frame, *args, **kwargs):
            if len(frame) < 26:
                return None
            return {
                "fresh_breakout": len(frame) == 26,
                "signal_pos": 25,
                "signal_date": "2026-01-01",
                "price": 100.0,
                "rsi_value": 40.0,
                "volume_ratio": None,
            }

        with patch.object(scanner, "detect_signal", side_effect=bullish), \
             patch.object(scanner, "detect_signal_bearish", return_value=None):
            result = scanner.backtest_signals(df, horizons=(5,))

        self.assertEqual(len(result["signals"]["bullish"]), 1)

    def test_bearish_backtest_treats_decline_as_profit(self):
        closes = [100.0] * 40
        closes[30] = 90.0
        df = pd.DataFrame({"Close": closes})

        def bearish(frame, *args, **kwargs):
            if len(frame) != 26:
                return None
            return {
                "fresh_breakout": True,
                "signal_pos": 25,
                "signal_date": "2026-01-01",
                "price": 100.0,
                "rsi_value": 60.0,
                "volume_ratio": None,
            }

        with patch.object(scanner, "detect_signal", return_value=None), \
             patch.object(scanner, "detect_signal_bearish", side_effect=bearish):
            result = scanner.backtest_signals(df, horizons=(5,))

        row = result["signals"]["bearish"][0]
        self.assertEqual(row["r5"], 10.0)
        self.assertEqual(result["summary"]["bearish"]["r5"]["win_rate"], 100.0)


if __name__ == "__main__":
    unittest.main()
