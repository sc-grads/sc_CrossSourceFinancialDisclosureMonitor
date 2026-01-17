# drift/test_detector.py
import sys
import os
import unittest
from datetime import datetime, timedelta

# Add current directory to path so we can import from local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from detector import detect_drift

class TestDetector(unittest.TestCase):
    def test_value_drift(self):
        # Setup
        ref_time = datetime(2024, 1, 1, 10, 0, 0)
        article_lookup = {
            "ref_id": ref_time,
            "obs_id": ref_time + timedelta(minutes=5)
        }
        
        claim_rows = [
            {
                "normalized_terms": "MSFT|EARNINGS|FY2024-Q2|EPS=2.93",
                "article_id": "ref_id",
                "source_name": "OFFICIAL"
            },
            {
                "normalized_terms": "MSFT|EARNINGS|FY2024-Q2|EPS=2.90", # Different value
                "article_id": "obs_id",
                "source_name": "NEWS_SITE"
            }
        ]
        
        events = detect_drift(claim_rows, article_lookup, "OFFICIAL")
        
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["drifts"][0]["type"], "VALUE_DRIFT")

    def test_numeric_equivalence(self):
        # Test that 2.930 equals 2.93
        ref_time = datetime(2024, 1, 1, 10, 0, 0)
        article_lookup = {
            "ref_id": ref_time,
            "obs_id": ref_time
        }
        
        claim_rows = [
            {
                "normalized_terms": "MSFT|EARNINGS|FY2024-Q2|EPS=2.93",
                "article_id": "ref_id",
                "source_name": "OFFICIAL"
            },
            {
                "normalized_terms": "MSFT|EARNINGS|FY2024-Q2|EPS=2.930", # Numerically equal
                "article_id": "obs_id",
                "source_name": "NEWS_SITE"
            }
        ]
        
        events = detect_drift(claim_rows, article_lookup, "OFFICIAL")
        self.assertEqual(len(events), 0) # Should be no drift

    def test_missing_article_lookup_safe(self):
        # Should NOT crash if article_id not in lookup, just log warning
        claim_rows = [
            {
                "normalized_terms": "MSFT|EARNINGS|FY2024-Q2|EPS=2.93",
                "article_id": "missing_id",
                "source_name": "OFFICIAL"
            }
        ]
        
        # Should run without error
        events = detect_drift(claim_rows, {}, "OFFICIAL")
        self.assertEqual(len(events), 0)

if __name__ == "__main__":
    unittest.main()
