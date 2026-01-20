# ingestion/ingest.py

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy.exc import IntegrityError

import finnhub
import yfinance as yf
import requests
from dotenv import load_dotenv

from ingestion.db import init_db, SessionLocal, Source, Article, Claim
from ingestion.health import mark_active, mark_offline
from ingestion.sources import SOURCES
from ingestion.normalizer import normalize

# ---- load .env from project root (consistent) ----
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)

from sentence_transformers import SentenceTransformer
_EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")


def embed(text: str) -> list[float]:
    vec = _EMBED_MODEL.encode(text or "", normalize_embeddings=True)
    return vec.tolist()


def sim_fail(flag_name: str) -> bool:
    return os.getenv(flag_name, "0").strip() == "1"


def upsert_sources(db) -> None:
    """
    Ensure SOURCES exist in the DB.
    IMPORTANT: Do NOT force status to active here.
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
                status="active",  # new sources default active
                last_successful_fetch=None,
            )
            db.add(row)
        else:
            row.source_name = name
            row.region = region

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


def preflight_finnhub(symbol: str) -> tuple[bool, str | None]:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return False, "FINNHUB_API_KEY missing"
    try:
        client = finnhub.Client(api_key=api_key)
        q = client.quote(symbol)
        if not isinstance(q, dict):
            return False, "Finnhub quote returned unexpected response"
        # Some symbols may return empty fields; still counts as API reachable.
        return True, None
    except Exception as e:
        return False, f"Finnhub preflight failed: {e}"


# ---------- NEWSAPI ----------
def _parse_newsapi_datetime(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def fetch_newsapi_news(query: str) -> list[dict]:
    api_key = os.getenv("NEWSAPI_KEY")
    if not api_key:
        raise ValueError("NEWSAPI_KEY missing in .env")

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "from": start.date().isoformat(),
        "to": end.date().isoformat(),
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 50,
        "apiKey": api_key,
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}

    if data.get("status") != "ok":
        raise RuntimeError(f"NewsAPI status={data.get('status')}")

    return data.get("articles") or []


def preflight_newsapi(query: str) -> tuple[bool, str | None]:
    api_key = os.getenv("NEWSAPI_KEY")
    if not api_key:
        return False, "NEWSAPI_KEY missing"
    try:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 1,
            "apiKey": api_key,
        }
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        data = r.json() or {}
        if data.get("status") != "ok":
            return False, f"NewsAPI status={data.get('status')}"
        return True, None
    except Exception as e:
        return False, f"NewsAPI preflight failed: {e}"


# ---------- ALPHAVANTAGE ----------
def _parse_av_time_published(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        if len(s) == 15:  # YYYYMMDDTHHMMSS
            dt = datetime.strptime(s, "%Y%m%dT%H%M%S")
        else:  # YYYYMMDDTHHMM
            dt = datetime.strptime(s, "%Y%m%dT%H%M")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def fetch_alphavantage_news(ticker: str, *, days: int = 7, limit: int = 50) -> list[dict]:
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise ValueError("ALPHAVANTAGE_API_KEY missing in .env")

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": ticker,
        "time_from": start.strftime("%Y%m%dT%H%M"),
        "sort": "LATEST",
        "limit": limit,
        "apikey": api_key,
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}

    feed = data.get("feed")
    if not isinstance(feed, list):
        note = data.get("Note") or data.get("Information") or data.get("Error Message")
        if note:
            raise RuntimeError(f"AlphaVantage API message: {note}")
        return []

    return feed


def preflight_alphavantage(ticker: str) -> tuple[bool, str | None]:
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        return False, "ALPHAVANTAGE_API_KEY missing"
    try:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "limit": 1,
            "apikey": api_key,
        }
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        data = r.json() or {}

        if data.get("feed") is None:
            note = data.get("Note") or data.get("Information") or data.get("Error Message")
            if note:
                return False, f"AlphaVantage message: {note}"

        return True, None
    except Exception as e:
        return False, f"AlphaVantage preflight failed: {e}"


# ---------- YFINANCE ----------
def fetch_yfinance_latest(symbol: str):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="5d")
    if df is None or df.empty:
        return None
    return df.iloc[-1]


def preflight_yfinance(symbol: str) -> tuple[bool, str | None]:
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="1d")
        if df is None or df.empty:
            return False, "yfinance returned empty history"
        return True, None
    except Exception as e:
        return False, f"yfinance preflight failed: {e}"


def add_article_and_claim(
    db,
    *,
    source_id: int,
    institution: str | None,
    title: str | None,
    url: str | None,
    published_at: datetime | None,
) -> bool:
    article = Article(
        source_id=source_id,
        institution=institution,
        title=title,
        url=url,
        published_at=published_at,
    )
    db.add(article)

    try:
        db.flush()
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


def preflight_source(src: dict) -> tuple[bool, str | None]:
    stype = (src.get("type") or "").lower()
    symbol = src.get("symbol") or ""
    query = src.get("query") or symbol

    if stype == "finnhub":
        if sim_fail("SIM_FAIL_FINNHUB"):
            return False, "Simulated Finnhub outage"
        return preflight_finnhub(symbol)

    if stype == "yfinance":
        if sim_fail("SIM_FAIL_YFINANCE"):
            return False, "Simulated YFinance outage"
        return preflight_yfinance(symbol)

    if stype == "newsapi":
        if sim_fail("SIM_FAIL_NEWSAPI"):
            return False, "Simulated NewsAPI outage"
        return preflight_newsapi(query)

    if stype == "alphavantage":
        if sim_fail("SIM_FAIL_ALPHAVANTAGE"):
            return False, "Simulated AlphaVantage outage"
        return preflight_alphavantage(symbol)

    return False, f"Unknown source type: {src.get('type')}"


def _phase1_preflight_update_statuses(db) -> None:
    """
    PHASE 1:
    Test every API first, then update Source.status accordingly.
    """
    print("Phase 1: Preflight sources (API availability checks)")
    for src in SOURCES:
        source_row = db.query(Source).filter_by(source_id=src["id"]).first()
        if not source_row:
            continue

        ok, reason = preflight_source(src)
        if ok:
            mark_active(db, source_row)
            print(f"✅ Preflight OK: {src.get('name')}")
        else:
            mark_offline(db, source_row, reason=reason)
            print(f"❌ Preflight FAIL: {src.get('name')} | {reason}")


def ingest():
    print("Pipeline started (PRE-FLIGHT -> STATUS UPDATE -> INGEST ACTIVE)")
    init_db()

    db = SessionLocal()
    try:
        upsert_sources(db)

        # PHASE 1: Preflight all sources and update statuses first
        _phase1_preflight_update_statuses(db)

        # PHASE 2: Only ingest sources that are currently active in DB
        print("Phase 2: Ingest only ACTIVE sources")
        for src in SOURCES:
            source_row = db.query(Source).filter_by(source_id=src["id"]).first()
            if not source_row:
                continue

            if (source_row.status or "").lower() != "active":
                print(f"⏭️ Skipping OFFLINE source: {src.get('name')}")
                continue

            try:
                print(f"Fetching {src.get('name')}")

                stype = (src.get("type") or "").lower()

                if stype == "finnhub":
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

                elif stype == "yfinance":
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

                    add_article_and_claim(
                        db,
                        source_id=source_row.source_id,
                        institution="Yahoo Finance",
                        title=title,
                        url=f"https://finance.yahoo.com/quote/{src['symbol']}",
                        published_at=latest_dt,
                    )

                elif stype == "newsapi":
                    q = src.get("query") or src["symbol"]
                    articles = fetch_newsapi_news(q)
                    for a in articles:
                        published = _parse_newsapi_datetime(a.get("publishedAt"))
                        source_name = ((a.get("source") or {}).get("name")) or "NewsAPI"
                        add_article_and_claim(
                            db,
                            source_id=source_row.source_id,
                            institution=source_name,
                            title=a.get("title"),
                            url=a.get("url"),
                            published_at=published,
                        )

                elif stype == "alphavantage":
                    items = fetch_alphavantage_news(src["symbol"])
                    for item in items:
                        published = _parse_av_time_published(item.get("time_published"))
                        add_article_and_claim(
                            db,
                            source_id=source_row.source_id,
                            institution=item.get("source"),
                            title=item.get("title"),
                            url=item.get("url"),
                            published_at=published,
                        )

                else:
                    raise Exception(f"Unknown source type: {src.get('type')}")

                db.commit()
                mark_active(db, source_row)
                print(f"✅ {src.get('name')} ingested (articles + claims)")

            except Exception as e:
                db.rollback()
                print(f"[WARN] {src.get('name')} failed during ingest: {e}")
                mark_offline(db, source_row, reason=str(e))

    finally:
        db.close()


if __name__ == "__main__":
    ingest()
