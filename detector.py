# drift/detector.py
from collections import defaultdict
import logging

try:
    from .parser import parse_normalized_terms
except ImportError:
    from parser import parse_normalized_terms

TIME_DRIFT_THRESHOLD_SECONDS = 15 * 60  # 15 minutes

logger = logging.getLogger(__name__)

def is_value_different(val1, val2):
    """
    Compare two values. Tries numeric comparison first, falls back to string.
    Returns True if different.
    """
    if val1 == val2:
        return False
        
    # Try numeric comparison to handle "1.0" vs "1"
    try:
        f1 = float(val1)
        f2 = float(val2)
        return abs(f1 - f2) > 1e-9
    except (ValueError, TypeError):
        pass
        
    return True

def detect_drift(claim_rows, article_lookup, authoritative_source):
    """
    claim_rows: list of {normalized_terms, article_id, source_name}
    article_lookup: dict article_id -> published_at (datetime object)
    """
    grouped = defaultdict(list)

    # Step 1 & 2: group by claim identity
    for row in claim_rows:
        try:
            parsed = parse_normalized_terms(row["normalized_terms"])
            identity = (
                parsed["entity"],
                parsed["claim_type"],
                parsed["scope"],
                parsed["key"]
            )
            grouped[identity].append({**row, **parsed})
        except ValueError as e:
            logger.warning(f"Skipping malformed term '{row.get('normalized_terms')}': {e}")
            continue

    drift_events = []

    # Step 3–4: compare against authoritative source
    for identity, claims in grouped.items():
        reference = [
            c for c in claims if c["source_name"] == authoritative_source
        ]

        if not reference:
            continue

        ref = reference[0]
        
        if ref["article_id"] not in article_lookup:
            logger.warning(f"Reference article ID {ref['article_id']} not found in lookup")
            continue
            
        ref_time = article_lookup[ref["article_id"]]

        for c in claims:
            if c["source_name"] == authoritative_source:
                continue

            event = {
                "identity": identity,
                "source": c["source_name"],
                "drifts": []
            }

            # Value drift
            if is_value_different(c["value"], ref["value"]):
                event["drifts"].append({
                    "type": "VALUE_DRIFT",
                    "ref": ref["value"],
                    "observed": c["value"]
                })

            # Timing drift
            if c["article_id"] in article_lookup:
                obs_time = article_lookup[c["article_id"]]
                # Ensure we have datetimes
                if hasattr(obs_time, 'total_seconds'): # It's a timedelta? No.
                    # Assume datetime objects
                    delta = (obs_time - ref_time).total_seconds()
                else:
                    # Try subtraction if they are datetimes
                    try:
                        delta = (obs_time - ref_time).total_seconds()
                    except AttributeError:
                        # Maybe they are strings?
                        logger.warning(f"Invalid datetime objects for comparison: {type(obs_time)} vs {type(ref_time)}")
                        continue

                if delta > TIME_DRIFT_THRESHOLD_SECONDS:
                    event["drifts"].append({
                        "type": "TIME_DRIFT",
                        "delay_seconds": delta
                    })
            else:
                logger.warning(f"Observed article ID {c['article_id']} not found in lookup")

            if event["drifts"]:
                drift_events.append(event)

    return drift_events
