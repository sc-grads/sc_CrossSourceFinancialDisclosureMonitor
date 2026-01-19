# ingestion/ingest.py

import os
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy.exc import IntegrityError

import finnhub
import yfinance as yf
from dotenv import load_dotenv

from ingestion.db import init_db, SessionLocal, Source, Article, Claim
from ingestion.health import mark_active, mark_offline
from ingestion.sources import SOURCES
from ingestion.normalizer import normalize

# ---- load .env from project root (consistent) ----
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)

# ✅ semantic model (loaded once)
from sentence_transformers import SentenceTransformer
_EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")


def embed(text: str) -> list[float]:
    """
    Returns a python list of floats so it can be stored in Postgres DOUBLE PRECISION[].
    """
    vec = _EMBED_MODEL.encode(text or "", normalize_embeddings=True)
    return vec.tolist()


def sim_fail(flag_name: str) -> bool:
    return os.getenv(flag_name, "0").strip() == "1"


def upsert_sources(db) -> None:
    """
    Ensure SOURCES exist in the DB (safe on first run and subsequent runs).
    """
    for src in SOURCES:
        src_id = int(src["id"])
        name = src.get("name")
        region = src.get("region")

        row = db.query(Source).filter_by(source_id=src_id).first()

        if row is None:
            row = Source(
                source_id=src_id,
                source_name=name,
                region=region,
                status="active",
                last_successful_fetch=None,
            )
            db.add(row)
        else:
            row.source_name = name
            row.region = region
            if row.status != "active":
                row.status = "active"

    db.commit()


# ---------- FINNHUB ----------
def fetch_finnhub_news(symbol: str) -> list[dict]:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY missing in .env")

    client = finnhub.Client(api_key=api_key)

    end = datetime.now()
    start = end - timedelta(days=7)

    return client.company_news(
        symbol,
        _from=start.strftime("%Y-%m-%d"),
        to=end.strftime("%Y-%m-%d"),
    )


# ---------- YFINANCE ----------
def fetch_yfinance_latest(symbol: str):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="5d")
    if df is None or df.empty:
        return None
    return df.iloc[-1]


def add_article_and_claim(
    db,
    *,
    source_id: int,
    institution: str | None,
    title: str | None,
    url: str | None,
    published_at: datetime | None,
) -> bool:
    """
    Inserts article + claim.
    Returns True if inserted, False if skipped as duplicate.
    """
    article = Article(
        source_id=source_id,
        institution=institution,
        title=title,
        url=url,
        published_at=published_at,
    )
    db.add(article)

    try:
        db.flush()  # assigns article_id; will raise IntegrityError if duplicate by unique index
    except IntegrityError:
        db.rollback()
        return False

    claim = Claim(
        article_id=article.article_id,
        normalized_terms=normalize(title or ""),
        embedding=embed(title or ""),
    )
    db.add(claim)
    return True


def ingest():
    print("Pipeline started (Finnhub + YFinance)")
    init_db()

    db = SessionLocal()
    try:
        upsert_sources(db)

        for src in SOURCES:
            source_row = db.query(Source).filter_by(source_id=src["id"]).first()
            if not source_row:
                print(f"❌ Source {src['name']} missing even after upsert")
                continue

            try:
                print(f"Fetching {src['name']}")

                if src["type"] == "finnhub":
                    if sim_fail("SIM_FAIL_FINNHUB"):
                        raise Exception("Simulated Finnhub outage")

                    items = fetch_finnhub_news(src["symbol"])
                    for item in items:
                        published = (
                            datetime.fromtimestamp(item.get("datetime"))
                            if item.get("datetime")
                            else None
                        )
                        add_article_and_claim(
                            db,
                            source_id=source_row.source_id,
                            institution=item.get("source"),
                            title=item.get("headline"),
                            url=item.get("url"),
                            published_at=published,
                        )

                elif src["type"] == "yfinance":
                    if sim_fail("SIM_FAIL_YFINANCE"):
                        raise Exception("Simulated YFinance outage")

                    latest = fetch_yfinance_latest(src["symbol"])
                    if latest is None:
                        raise Exception("yfinance returned no data")

                    try:
                        latest_dt = latest.name.to_pydatetime()
                    except Exception:
                        latest_dt = None

                    close_price = float(latest["Close"])
                    volume = int(latest["Volume"]) if "Volume" in latest else None

                    title = f"{src['symbol']} yfinance close={close_price} volume={volume}"

                    inserted = add_article_and_claim(
                        db,
                        source_id=source_row.source_id,
                        institution="Yahoo Finance",
                        title=title,
                        url=f"https://finance.yahoo.com/quote/{src['symbol']}",
                        published_at=latest_dt,
                    )
                    if not inserted:
                        continue
                else:
                    raise Exception(f"Unknown source type: {src.get('type')}")

                db.commit()
                mark_active(db, source_row)
                print(f"✅ {src['name']} ingested (articles + claims)")

            except Exception as e:
                db.rollback()
                print(f"[WARN] {src['name']} failed: {e}")
                mark_offline(db, source_row)

    finally:
        db.close()


if __name__ == "__main__":
    ingest()
