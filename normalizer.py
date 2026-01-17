# normalizer.py
from rules import normalize_eps, normalize_revenue, normalize_period
import logging

logger = logging.getLogger(__name__)

def build_claim(entity, claim_type, scope, key, value):
    """Build a normalized claim string."""
    return f"{entity}|{claim_type}|{scope}|{key}={value}"

def normalize_article(article):
    """
    Input: article row from DB (dict with keys: institution, title, eps, revenue)
    Output: list of normalized claim strings
    
    Returns empty list if article is not eligible for normalization.
    """
    claims = []
    
    # Validate article structure
    if not isinstance(article, dict):
        logger.warning(f"Invalid article type: {type(article)}")
        return claims
    
    institution = article.get("institution")
    title = article.get("title") or ""
    
    if not institution:
        logger.warning("Article missing institution field")
        return claims

    # --- classify disclosure type ---
    title_lower = title.lower()
    
    if "earnings" in title_lower:
        claim_type = "EARNINGS"
    elif "trading halt" in title_lower:
        claim_type = "HALT"
    else:
        logger.debug(f"Article not eligible for normalization: {title}")
        return claims  # not eligible

    # --- earnings normalization ---
    if claim_type == "EARNINGS":
        try:
            period = normalize_period(title)
        except ValueError as e:
            logger.error(f"Failed to extract period from title '{title}': {e}")
            return claims  # Cannot process without period
        
        # Process EPS
        if article.get("eps"):
            try:
                eps_value = article["eps"]
                # Convert to string if it's already a number
                if isinstance(eps_value, (int, float)):
                    eps_value = str(eps_value)
                
                eps = normalize_eps(eps_value)
                claims.append(
                    build_claim(
                        institution,
                        "EARNINGS",
                        period,
                        "EPS",
                        str(eps)  # Explicitly convert Decimal to string
                    )
                )
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to normalize EPS '{article.get('eps')}': {e}")
        
        # Process Revenue
        if article.get("revenue"):
            try:
                revenue_value = article["revenue"]
                # Convert to string if it's already a number
                if isinstance(revenue_value, (int, float)):
                    revenue_value = f"${revenue_value}"
                
                revenue = normalize_revenue(revenue_value)
                claims.append(
                    build_claim(
                        institution,
                        "EARNINGS",
                        period,
                        "REVENUE_USD",
                        str(revenue)  # Explicitly convert int to string
                    )
                )
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to normalize revenue '{article.get('revenue')}': {e}")

    # --- trading halt normalization ---
    elif claim_type == "HALT":
        claims.append(
            build_claim(
                institution,
                "HALT",
                "NASDAQ",
                "STATUS",
                "HALTED"
            )
        )

    return claims
