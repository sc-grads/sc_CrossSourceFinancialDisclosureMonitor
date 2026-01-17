# drift/test_parser.py
from parser import parse_normalized_terms
import unittest

class TestParser(unittest.TestCase):
    def test_valid_input(self):
        term = "MSFT|EARNINGS|FY2024-Q2|EPS=2.93"
        expected = {
            "entity": "MSFT",
            "claim_type": "EARNINGS",
            "scope": "FY2024-Q2",
            "key": "EPS",
            "value": "2.93"
        }
        self.assertEqual(parse_normalized_terms(term), expected)

    def test_malformed_missing_pipe(self):
        # Missing one pipe - should still fail
        term = "MSFT|EARNINGS|EPS=2.93"
        with self.assertRaises(ValueError):
            parse_normalized_terms(term)

    def test_extra_pipe_in_value(self):
        # Value contains a pipe - should now SUCCEED
        term = "MSFT|EARNINGS|FY2024-Q2|NOTE=Some|Value"
        expected = {
            "entity": "MSFT",
            "claim_type": "EARNINGS",
            "scope": "FY2024-Q2",
            "key": "NOTE",
            "value": "Some|Value"
        }
        self.assertEqual(parse_normalized_terms(term), expected)

    def test_value_contains_equals(self):
        # Value contains an equals sign - should now SUCCEED
        term = "MSFT|CONFIG|GLOBAL|URL=https://example.com?q=1"
        expected = {
            "entity": "MSFT",
            "claim_type": "CONFIG",
            "scope": "GLOBAL",
            "key": "URL",
            "value": "https://example.com?q=1"
        }
        self.assertEqual(parse_normalized_terms(term), expected)

    def test_whitespace_handling(self):
        # Whitespace should be stripped
        term = " MSFT | EARNINGS | FY2024-Q2 | EPS = 2.93 "
        expected = {
            "entity": "MSFT",
            "claim_type": "EARNINGS",
            "scope": "FY2024-Q2",
            "key": "EPS",
            "value": "2.93"
        }
        self.assertEqual(parse_normalized_terms(term), expected)

if __name__ == "__main__":
    unittest.main()
