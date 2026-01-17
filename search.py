import os
import psycopg2
from dotenv import load_dotenv

from normalizer import normalize

load_dotenv()  # loads .env from project root if you run from root


# -------------------------
# STEP 3.8: Grouped Search
# -------------------------

SQL_TOP_CLAIM = """
WITH input_tokens AS (
  SELECT unnest(string_to_array(%s, ' ')) AS tok
),
claim_tokens AS (
  SELECT
    c.claim_id,
    c.normalized_terms,
    unnest(string_to_array(c.normalized_terms, ' ')) AS tok
  FROM claims c
),
overlap AS (
  SELECT
    ct.claim_id,
    ct.normalized_terms,
    COUNT(*) AS overlap_count
  FROM claim_tokens ct
  JOIN input_tokens it ON it.tok = ct.tok
  GROUP BY ct.claim_id, ct.normalized_terms
)
SELECT
  normalized_terms,
  overlap_count
FROM overlap
WHERE overlap_count >= %s
ORDER BY overlap_count DESC
LIMIT 1;
"""

SQL_SUPPORT = """
WITH totals AS (
  SELECT COUNT(*)::numeric AS total_sources FROM sources
),
support AS (
  SELECT COUNT(DISTINCT a.source_id)::numeric AS sources_supporting
  FROM claims c
  JOIN articles a ON a.article_id = c.article_id
  WHERE c.normalized_terms = %s
)
SELECT
  support.sources_supporting,
  totals.total_sources,
  ROUND(support.sources_supporting / totals.total_sources, 3) AS support_ratio
FROM support, totals;
"""

SQL_EVIDENCE = """
SELECT
  s.source_name,
  a.institution,
  a.title,
  a.url,
  a.published_at
FROM claims c
JOIN articles a ON a.article_id = c.article_id
JOIN sources  s ON s.source_id = a.source_id
WHERE c.normalized_terms = %s
ORDER BY a.published_at DESC;
"""


def get_conn():
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")

    missing = [k for k, v in {
        "DB_NAME": dbname,
        "DB_USER": user,
        "DB_PASSWORD": password,
        "DB_HOST": host,
        "DB_PORT": port,
    }.items() if not v]

    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")

    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )


def search_grouped(user_input: str, min_overlap: int = 3):
    """
    1) Normalize the user input
    2) Find the best-matching claim by overlap score
    3) Compute sources_supporting + support_ratio
    4) Return supporting evidence articles
    """
    normalized_input = normalize(user_input)

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) Find best claim match
            cur.execute(SQL_TOP_CLAIM, (normalized_input, min_overlap))
            top = cur.fetchone()

            if not top:
                return {
                    "user_input": user_input,
                    "normalized_input": normalized_input,
                    "match_found": False,
                    "message": f"No claim found with overlap >= {min_overlap}",
                    "evidence": []
                }

            matched_claim, best_overlap = top

            # 2) Compute support ratio
            cur.execute(SQL_SUPPORT, (matched_claim,))
            sources_supporting, total_sources, support_ratio = cur.fetchone()

            # 3) Fetch evidence articles
            cur.execute(SQL_EVIDENCE, (matched_claim,))
            evidence_rows = cur.fetchall()

    # Convert evidence rows into dicts (JSON-friendly)
    evidence = []
    for source_name, institution, title, url, published_at in evidence_rows:
        evidence.append({
            "source_name": source_name,
            "institution": institution,
            "title": title,
            "url": url,
            "published_at": published_at.isoformat() if published_at else None
        })

    return {
        "user_input": user_input,
        "normalized_input": normalized_input,
        "match_found": True,
        "matched_claim": matched_claim,
        "best_overlap": int(best_overlap),
        "sources_supporting": int(sources_supporting),
        "total_sources": int(total_sources),
        "support_ratio": float(support_ratio),
        "evidence": evidence
    }


if __name__ == "__main__":
    print("Interactive search mode")
    print("Type a headline and press Enter")
    print("Type 'exit' to quit")

    while True:
        user_title = input("\nHeadline> ").strip()

        if user_title.lower() == "exit":
            print("Exiting...")
            break

        grouped = search_grouped(user_title, min_overlap=3)

        print("\nGROUPED RESULT (API-ready):")
        print(grouped)
