# ingestion/run_title_search.py

"""
Entry point to handle a user-provided headline.

Behavior:
1) Search existing claims (semantic search)
2) If no match:
   - resolve a ticker symbol from the title
   - update sources to use that ticker
   - ingest from Finnhub + YFinance (both)
   - search again
3) Print one API-ready dict consistently
"""

import sys
from datetime import datetime

from ingestion.ingest import ingest
from ingestion.search import search_grouped
from ingestion.sources import SOURCES
from ingestion.symbol_resolver import resolve_symbol_from_title


def update_sources_symbol(new_symbol: str):
    """
    Update the symbol for all sources in SOURCES.

    Mutates SOURCES in-place so subsequent ingest() uses the new ticker.
    """
    for src in SOURCES:
        src["symbol"] = new_symbol

        # Rename source for readability (IDs stay stable)
        if src.get("type") == "finnhub":
            src["name"] = f"Finnhub_{new_symbol}"
        elif src.get("type") == "yfinance":
            src["name"] = f"YFinance_{new_symbol}"


def build_response(*, stage: str, title: str, base_result: dict, **extra) -> dict:
    """
    Ensures a consistent response format for API clients.
    """
    resp = {
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "stage": stage,  # e.g. "initial_search", "post_ingestion_search", "no_symbol"
        "user_input": title,
        "normalized_input": base_result.get("normalized_input"),
        "match_found": bool(base_result.get("match_found")),
        "message": base_result.get("message"),
    }

    # Copy standard fields if present
    for k in [
        "matched_claim",
        "best_similarity",
        "negation_conflict",
        "sources_supporting",
        "total_sources",
        "support_ratio",
        "evidence",
    ]:
        if k in base_result:
            resp[k] = base_result[k]

    # Add extras
    resp.update(extra)
    # Ensure evidence always exists for API consistency
    if "evidence" not in resp:
        resp["evidence"] = []

    return resp


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m ingestion.run_title_search \"<headline>\"")
        sys.exit(1)

    title = sys.argv[1]

    # Set your semantic threshold here (keep consistent)
    min_similarity = 0.75

    # 1) Search existing claims
    result1 = search_grouped(title, min_similarity=min_similarity)
    if result1.get("match_found"):
        out = build_response(
            stage="initial_search",
            title=title,
            base_result=result1,
            ingestion_ran=False,
            resolved_symbol=None,
            min_similarity=min_similarity,
        )
        print("\nGROUPED RESULT (API-ready):")
        print(out)
        return

    # 2) Resolve ticker from title
    symbol = resolve_symbol_from_title(title)
    if not symbol:
        out = build_response(
            stage="no_symbol",
            title=title,
            base_result=result1,
            ingestion_ran=False,
            resolved_symbol=None,
            min_similarity=min_similarity,
            message="No claim found, and could not resolve a company ticker from the title.",
        )
        print("\nGROUPED RESULT (API-ready):")
        print(out)
        return

    # 3) Update sources + ingest
    update_sources_symbol(symbol)
    print(f"[INFO] Resolved symbol '{symbol}'. Re-running ingestion (Finnhub + YFinance)...")
    ingest()

    # 4) Search again
    result2 = search_grouped(title, min_similarity=min_similarity)

    if result2.get("match_found"):
        out = build_response(
            stage="post_ingestion_search",
            title=title,
            base_result=result2,
            ingestion_ran=True,
            resolved_symbol=symbol,
            min_similarity=min_similarity,
        )
    else:
        out = build_response(
            stage="post_ingestion_search_no_match",
            title=title,
            base_result=result2,
            ingestion_ran=True,
            resolved_symbol=symbol,
            min_similarity=min_similarity,
            message="No claim found after ingesting the resolved company data.",
        )

    print("\nGROUPED RESULT (API-ready):")
    print(out)


if __name__ == "__main__":
    main()
