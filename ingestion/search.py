# ingestion/search.py
 
import os
import psycopg2
import numpy as np
from dotenv import load_dotenv
 
from ingestion.normalizer import normalize, has_negation
from ingestion.ingest import ingest
from ingestion.sources import SOURCES
from ingestion.symbol_resolver import resolve_symbol_from_title
 
from sentence_transformers import SentenceTransformer
 
load_dotenv()
_EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
 
 
def embed(text: str) -> np.ndarray:
    vec = _EMBED_MODEL.encode(text or "", normalize_embeddings=True)
    return np.asarray(vec, dtype=np.float32)
 
 
def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b))
 
 
# -------------------------
# DB SQL (ACTIVE SOURCES ONLY)
# -------------------------
 
SQL_ACTIVE_TOTAL = """
SELECT COUNT(*)::int
FROM sources
WHERE status = 'active';
"""
 
SQL_SUPPORT_ACTIVE = """
WITH totals AS (
  SELECT COUNT(*)::numeric AS total_sources
  FROM sources
  WHERE status = 'active'
),
support AS (
  SELECT COUNT(DISTINCT a.source_id)::numeric AS sources_supporting
  FROM claims c
  JOIN articles a ON a.article_id = c.article_id
  JOIN sources  s ON s.source_id = a.source_id
  WHERE c.normalized_terms = %s
    AND s.status = 'active'
)
SELECT
  support.sources_supporting,
  totals.total_sources,
  CASE
    WHEN totals.total_sources = 0 THEN 0
    ELSE ROUND(support.sources_supporting / totals.total_sources, 3)
  END AS support_ratio
FROM support, totals;
"""
 
SQL_EVIDENCE_ACTIVE = """
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
  AND s.status = 'active'
ORDER BY a.published_at DESC;
"""
 
SQL_ALL_CLAIM_EMBEDS = """
SELECT
  c.normalized_terms,
  c.embedding
FROM claims c
WHERE c.embedding IS NOT NULL
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
 
 
def update_sources_symbol(new_symbol: str):
    """
    Update SOURCES in-place so ingest() fetches for that symbol.
    """
    for src in SOURCES:
        src["symbol"] = new_symbol
        if src.get("type") == "finnhub":
            src["name"] = f"Finnhub_{new_symbol}"
        elif src.get("type") == "yfinance":
            src["name"] = f"YFinance_{new_symbol}"
 
 
def find_best_semantic_match(cur, user_input: str, min_similarity: float):
    """
    Returns (matched_claim_normalized_terms, best_similarity) or (None, None)
    """
    user_vec = embed(user_input)
 
    cur.execute(SQL_ALL_CLAIM_EMBEDS)
    rows = cur.fetchall()
 
    best_claim = None
    best_score = -1.0
 
    for normalized_terms, emb in rows:
        if not emb:
            continue
        emb_vec = np.asarray(emb, dtype=np.float32)
        score = cosine_sim(user_vec, emb_vec)
        if score > best_score:
            best_score = score
            best_claim = normalized_terms
 
    if best_claim is None or best_score < min_similarity:
        return None, None
 
    return best_claim, best_score
 
 
def _get_active_total_sources(cur) -> int:
    cur.execute(SQL_ACTIVE_TOTAL)
    return int(cur.fetchone()[0] or 0)
 
 
def search_grouped(user_input: str, min_similarity: float = 0.75):
    """
    Semantic grouped search (ACTIVE sources only for totals + support + evidence)
    """
    normalized_input = normalize(user_input)
 
    with get_conn() as conn:
        with conn.cursor() as cur:
            total_active_sources = _get_active_total_sources(cur)
 
            matched_claim, best_sim = find_best_semantic_match(cur, user_input, min_similarity)
 
            if not matched_claim:
                return {
                    "user_input": user_input,
                    "normalized_input": normalized_input,
                    "match_found": False,
                    "message": f"No semantic match found with similarity >= {min_similarity}",
                    "evidence": [],
                    "sources_supporting": 0,
                    "total_sources": int(total_active_sources),
                    "support_ratio": 0.0,
                }
 
            user_neg = has_negation(user_input)
            claim_neg = has_negation(matched_claim)
            negation_conflict = (user_neg != claim_neg)
 
            if negation_conflict and (best_sim or 0) < 0.88:
                return {
                    "user_input": user_input,
                    "normalized_input": normalized_input,
                    "match_found": False,
                    "message": "Possible contradiction detected (negation mismatch). No strong semantic match found.",
                    "evidence": [],
                    "sources_supporting": 0,
                    "total_sources": int(total_active_sources),
                    "support_ratio": 0.0,
                }
 
            cur.execute(SQL_SUPPORT_ACTIVE, (matched_claim,))
            sources_supporting, total_sources, support_ratio = cur.fetchone()
 
            cur.execute(SQL_EVIDENCE_ACTIVE, (matched_claim,))
            evidence_rows = cur.fetchall()
 
    evidence = []
    for source_name, institution, title, url, published_at in evidence_rows:
        evidence.append({
            "source_name": source_name,
            "institution": institution,
            "title": title,
            "url": url,
            "published_at": published_at.isoformat() if published_at else None,
        })
 
    return {
        "user_input": user_input,
        "normalized_input": normalized_input,
        "match_found": True,
        "matched_claim": matched_claim,
        "best_similarity": float(round(best_sim, 4)) if best_sim is not None else None,
        "negation_conflict": bool(negation_conflict),
        "sources_supporting": int(sources_supporting or 0),
        "total_sources": int(total_sources or 0),
        "support_ratio": float(support_ratio or 0.0),
        "evidence": evidence,
    }
 
 
def search_with_auto_ingest(user_input: str, min_similarity: float = 0.75) -> dict:
    """
    - Search DB
    - If not found:
        resolve symbol
        ingest for that symbol
        search again
    """
    result1 = search_grouped(user_input, min_similarity=min_similarity)
    if result1.get("match_found"):
        result1["ingestion_ran"] = False
        result1["resolved_symbol"] = None
        return result1
 
    symbol = resolve_symbol_from_title(user_input)
    if not symbol:
        result1["ingestion_ran"] = False
        result1["resolved_symbol"] = None
        result1["message"] = "No match found and could not resolve a company ticker from the headline."
        return result1
 
    print("Waiting... (ingesting fresh data)")
    update_sources_symbol(symbol)
    ingest()
 
    result2 = search_grouped(user_input, min_similarity=min_similarity)
    result2["ingestion_ran"] = True
    result2["resolved_symbol"] = symbol
 
    if not result2.get("match_found"):
        result2["message"] = "No match found after ingestion for resolved company."
    return result2
 
 
if __name__ == "__main__":
    print("Interactive search mode (semantic + auto-ingest)")
    print("Type a headline and press Enter")
    print("Type 'exit' to quit")
 
    MIN_SIMILARITY = 0.75
 
    while True:
        user_title = input("\nHeadline> ").strip()
        if user_title.lower() == "exit":
            print("Exiting...")
            break
 
        final_result = search_with_auto_ingest(user_title, min_similarity=MIN_SIMILARITY)
 
        print("\nGROUPED RESULT (API-ready):")
        print(final_result)