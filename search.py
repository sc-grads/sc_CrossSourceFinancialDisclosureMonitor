import os
import psycopg2
from dotenv import load_dotenv

from normalizer import normalize

load_dotenv()  # loads .env from project root if you run from root

SQL = """
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
  o.overlap_count,
  s.source_name,
  a.institution,
  a.title,
  a.url,
  a.published_at
FROM overlap o
JOIN claims   c ON c.claim_id = o.claim_id
JOIN articles a ON a.article_id = c.article_id
JOIN sources  s ON s.source_id = a.source_id
WHERE o.overlap_count >= %s
ORDER BY o.overlap_count DESC, a.published_at DESC
LIMIT %s;
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

def search_by_title(user_input: str, min_overlap: int = 3, limit: int = 10):
    normalized_input = normalize(user_input)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL, (normalized_input, min_overlap, limit))
            rows = cur.fetchall()

    return normalized_input, rows

def rows_to_dicts(rows):
    """
    Convert raw DB tuples into JSON-friendly dictionaries.
    This is what your extension/API will consume.
    """
    results = []
    for overlap_count, source_name, institution, title, url, published_at in rows:
        results.append({
            "overlap_count": int(overlap_count),
            "source_name": source_name,
            "institution": institution,
            "title": title,
            "url": url,
            "published_at": published_at.isoformat() if published_at else None
        })
    return results

if __name__ == "__main__":
    # Change this text to whatever the user types in your extension later
    user_title = "Apple and Goldman Sachs end credit card partnership"

    normalized, rows = search_by_title(user_title, min_overlap=3, limit=10)

    # ✅ Step 3.7 output: structured results
    structured = rows_to_dicts(rows)

    print("User input:", user_title)
    print("Normalized:", normalized)
    print("\nStructured results (API-ready):")
    for item in structured:
        print(item)

    # --- Optional: keep your old pretty printing (if you still want it) ---
    # print("\nTop matches:")
    # for overlap_count, source_name, institution, title, url, published_at in rows:
    #     print(f"- overlap={overlap_count} | {source_name} | {institution} | {title}")
    #     print(f"  {url}")
    #     print(f"  {published_at}\n")
