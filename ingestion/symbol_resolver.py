# ingestion/symbol_resolver.py

import os
from pathlib import Path
from dotenv import load_dotenv

# ---- load .env from project root (consistent) ----
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)

# Simple fallback mapping (fast path)
COMPANY_SYMBOL_MAP = {
    "apple": "AAPL",
    "apple inc": "AAPL",
    "microsoft": "MSFT",
    "msft": "MSFT",
    "google": "GOOGL",
    "alphabet": "GOOGL",
    "google inc": "GOOGL",
    "amazon": "AMZN",
    "amazon.com": "AMZN",
    "amazon inc": "AMZN",
    "facebook": "META",
    "meta": "META",
    "meta platforms": "META",
    "tesla": "TSLA",
    "tesla inc": "TSLA",
    "netflix": "NFLX",
    "nvidia": "NVDA",
    "intel": "INTC",
    "amd": "AMD",
    "advanced micro devices": "AMD",
    "ibm": "IBM",
    "international business machines": "IBM",
    "oracle": "ORCL",
    "salesforce": "CRM",
    "salesforce.com": "CRM",
    "adobe": "ADBE",
    "paypal": "PYPL",
    "qualcomm": "QCOM",
    "cisco": "CSCO",
    "uber": "UBER",
    "lyft": "LYFT",
    "airbnb": "ABNB",
    "zoom": "ZM",
    "zoom video": "ZM",
    "zoom video communications": "ZM",
    "spotify": "SPOT",
    "shopify": "SHOP",
    "square": "SQ",
    "block": "SQ",
    "twitter": "X",
    "x": "X",

    "walmart": "WMT",
    "wal-mart": "WMT",
    "target": "TGT",
    "costco": "COST",
    "home depot": "HD",
    "the home depot": "HD",
    "lowes": "LOW",
    "lowe's": "LOW",
    "nike": "NKE",
    "coca cola": "KO",
    "coca-cola": "KO",
    "pepsi": "PEP",
    "pepsico": "PEP",
    "mcdonalds": "MCD",
    "mcdonald's": "MCD",
    "starbucks": "SBUX",
    "disney": "DIS",
    "walt disney": "DIS",

    "visa": "V",
    "mastercard": "MA",
    "american express": "AXP",
    "amex": "AXP",
    "jpmorgan": "JPM",
    "jp morgan": "JPM",
    "bank of america": "BAC",
    "bofa": "BAC",
    "goldman sachs": "GS",
    "morgan stanley": "MS",
    "wells fargo": "WFC",

    "ford": "F",
    "ford motor": "F",
    "general motors": "GM",
    "gm": "GM",
    "toyota": "TM",
    "volkswagen": "VWAGY",
    "vw": "VWAGY",

    "exxon": "XOM",
    "exxon mobil": "XOM",
    "chevron": "CVX",
    "shell": "SHEL",
    "bp": "BP",

    "asml": "ASML",
    "tsmc": "TSM",
    "taiwan semiconductor": "TSM",
    "broadcom": "AVGO",

    "sony": "SONY",
    "netflix inc": "NFLX",
    "warner bros": "WBD",
    "warner brothers": "WBD",

    "coinbase": "COIN",
    "coinbase global": "COIN",
    "riot": "RIOT",
    "riot blockchain": "RIOT",
    "marathon": "MARA",
    "marathon digital": "MARA",
}


def _finnhub_lookup_symbol(company_hint: str) -> str | None:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return None

    try:
        import finnhub
        client = finnhub.Client(api_key=api_key)
        res = client.symbol_lookup(company_hint)
        items = (res or {}).get("result") or []
        if not items:
            return None

        preferred = []
        for it in items:
            sym = (it.get("symbol") or "").strip()
            desc = (it.get("description") or "").lower()
            typ = (it.get("type") or "").lower()

            score = 0
            if sym and sym.isalpha():
                score += 2
            if "united states" in desc or "usa" in desc or " us" in desc:
                score += 2
            if "common stock" in desc:
                score += 2
            if typ in ("common stock", "equity"):
                score += 1

            if sym:
                preferred.append((score, sym))

        preferred.sort(reverse=True, key=lambda x: x[0])
        return preferred[0][1] if preferred else None

    except Exception:
        return None


def resolve_symbol_from_title(title: str) -> str | None:
    if not title:
        return None

    lowered = title.lower()

    for keyword, symbol in COMPANY_SYMBOL_MAP.items():
        if keyword in lowered:
            return symbol

    return _finnhub_lookup_symbol(title)
