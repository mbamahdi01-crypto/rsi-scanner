import tempfile
import unittest
from pathlib import Path

import build_data


class BuildDataTests(unittest.TestCase):
    def test_preferred_share_symbol_is_normalized_for_yahoo(self):
        self.assertEqual(build_data._normalize_ticker("ABR$D"), "ABR-PD")

    def test_placeholder_symbol_is_rejected(self):
        self.assertEqual(build_data._normalize_ticker("-"), "")

    def test_incomplete_refresh_is_rejected_without_replacing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "stocks.json"
            path.write_text('{"stocks":[{"ticker":"OLD"}]}', encoding="utf-8")
            with self.assertRaises(ValueError):
                build_data._atomic_dump(str(path), {"stocks": []}, 1)
            self.assertIn("OLD", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
