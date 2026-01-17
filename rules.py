# rules.py
import re
from decimal import Decimal, InvalidOperation

EPS_PATTERN = re.compile(r"EPS\s*\$?([\d\.]+)", re.I)
REVENUE_PATTERN = re.compile(r"\$?([\d,]+\.?\d*)\s*(B|billion|M|million)", re.I)

def normalize_eps(raw_eps: str) -> Decimal:
    """
    Extract and normalize EPS value from string.
    Handles formats like: "1.23", "$1.23", "EPS $1.23"
    """
    # Try to extract using pattern first
    match = EPS_PATTERN.search(raw_eps)
    if match:
        raw_eps = match.group(1)
    
    # Remove common formatting characters
    cleaned = raw_eps.strip().replace('$', '').replace(',', '')
    
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"Invalid EPS format: {raw_eps}") from e

def normalize_revenue(raw_rev: str) -> int:
    """
    Extract and normalize revenue from string.
    Handles formats like: "$1.5B", "1.5 billion", "$500M", "500 million"
    """
    match = REVENUE_PATTERN.search(raw_rev)
    if not match:
        raise ValueError(f"Revenue format not recognized: {raw_rev}")

    try:
        # Remove commas from the number
        value_str = match.group(1).replace(',', '')
        value = Decimal(value_str)
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"Invalid revenue number: {match.group(1)}") from e

    unit = match.group(2).lower()

    if unit.startswith("b"):
        return int(value * Decimal(1_000_000_000))
    if unit.startswith("m"):
        return int(value * Decimal(1_000_000))

    raise ValueError(f"Unknown revenue unit: {unit}")

def normalize_period(raw: str) -> str:
    """
    Normalize fiscal period to standard format: FY{YYYY}-Q{N}
    
    Examples:
    - "Q2 2024" -> "FY2024-Q2"
    - "FY24 Q2" -> "FY2024-Q2"
    - "2024-Q2" -> "FY2024-Q2"
    - "Q2 FY24" -> "FY2024-Q2"
    """
    # Try to match Q[1-4] and a year (2-digit or 4-digit)
    m = re.search(r"Q([1-4]).*?(\d{2}|\d{4})", raw, re.I)
    if not m:
        raise ValueError(f"Unrecognized period format: {raw}")

    quarter = m.group(1)
    year = m.group(2)
    
    # Convert 2-digit year to 4-digit (assume 20xx)
    if len(year) == 2:
        year = f"20{year}"

    return f"FY{year}-Q{quarter}"
