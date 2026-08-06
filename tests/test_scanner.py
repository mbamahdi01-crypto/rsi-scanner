import unittest
from unittest.mock import patch

import pandas as pd

import scanner


class ScannerTests(unittest.TestCase):
    @staticmethod
    def _triple_frames():
        close = [10.0] * 59 + [20.0]
        frame = pd.DataFrame({
            "Date": pd.date_range("2026-01-01", periods=60, freq="D"),
            "Open": close, "High": [v + 1 for v in close],
            "Low": [v - 1 for v in close], "Close": close,
        })
        return frame.copy(), frame.copy(), frame.copy()

    def test_triple_filter_requires_real_macd_cross(self):
        large, medium, small = self._triple_frames()
        macd = pd.Series([-2.0] * 59 + [-1.5])
        signal = pd.Series([-1.0] * 60)
        histogram = macd - signal
        rsi = pd.Series([60.0] * 60)
        stoch_k = pd.Series([20.0] * 59 + [40.0])
        stoch_d = pd.Series([30.0] * 60)

        with patch.object(scanner, "calculate_macd", return_value=(macd, signal, histogram)), \
             patch.object(scanner, "calculate_rsi", return_value=rsi), \
             patch.object(scanner, "calculate_stochastic", return_value=(stoch_k, stoch_d)):
            result = scanner.detect_triple_filter(large, medium, small)

        self.assertIsNone(result)

    def test_triple_filter_accepts_all_three_rules(self):
        large, medium, small = self._triple_frames()
        macd = pd.Series([-2.0] * 59 + [-1.0])
        signal = pd.Series([-1.5] * 59 + [-1.2])
        histogram = macd - signal
        rsi = pd.Series([60.0] * 60)
        stoch_k = pd.Series([20.0] * 59 + [40.0])
        stoch_d = pd.Series([30.0] * 60)

        with patch.object(scanner, "calculate_macd", return_value=(macd, signal, histogram)), \
             patch.object(scanner, "calculate_rsi", return_value=rsi), \
             patch.object(scanner, "calculate_stochastic", return_value=(stoch_k, stoch_d)):
            result = scanner.detect_triple_filter(large, medium, small)

        self.assertIsNotNone(result)
        self.assertEqual(result["direction"], "triple_bullish")

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
