import os
from datetime import datetime, timedelta

import finnhub
import requests
import yfinance as yf
from dotenv import load_dotenv

from db import init_db, SessionLocal, Source, Article, Claim
from health import mark_active, mark_offline
from sources import SOURCES
from normalizer import normalize

load_dotenv()


def sim_fail(flag_name: str) -> bool:
    return os.getenv(flag_name, "0").strip() == "1"


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
        to=end.strftime("%Y-%m-%d")
    )


# ---------- YFINANCE ----------
def fetch_yfinance_latest(symbol: str):
    """
    Pull last few days of market data and return latest row.
    """
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="5d")
    if df is None or df.empty:
        return None
    return df.iloc[-1]  # latest row


def add_article_and_claim(db, *, source_id: int, institution: str | None, title: str | None,
                          url: str | None, published_at: datetime | None) -> None:
    article = Article(
        source_id=source_id,
        institution=institution,
        title=title,
        url=url,
        published_at=published_at
    )
    db.add(article)
    db.flush()  # gives article_id

    claim = Claim(
        article_id=article.article_id,
        normalized_terms=normalize(title or "")
    )
    db.add(claim)


def ingest():
    print("Pipeline started (Finnhub + YFinance)")
    init_db()

    db = SessionLocal()
    try:
        for src in SOURCES:
            source_row = db.query(Source).filter_by(source_id=src["id"]).first()
            if not source_row:
                print(f"❌ Source {src['name']} not in DB (insert sources first)")
                continue

            try:
                print(f"Fetching {src['name']}")

                # --- Finnhub ---
                if src["type"] == "finnhub":
                    if sim_fail("SIM_FAIL_FINNHUB"):
                        raise Exception("Simulated Finnhub outage")

                    items = fetch_finnhub_news(src["symbol"])
                    for item in items:
                        published = (
                            datetime.fromtimestamp(item.get("datetime"))
                            if item.get("datetime") else None
                        )

                        add_article_and_claim(
                            db,
                            source_id=source_row.source_id,
                            institution=item.get("source"),
                            title=item.get("headline"),
                            url=item.get("url"),
                            published_at=published
                        )

                # --- YFinance ---
                elif src["type"] == "yfinance":
                    if sim_fail("SIM_FAIL_YFINANCE"):
                        raise Exception("Simulated YFinance outage")

                    latest = fetch_yfinance_latest(src["symbol"])
                    if latest is None:
                        raise Exception("yfinance returned no data (market closed or symbol invalid)")

                    # df row index is a timestamp in most cases
                    # yfinance returns a timezone-aware Timestamp; convert to python datetime cleanly
                    # We’ll treat that as published_at
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
                        published_at=latest_dt
                    )

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
