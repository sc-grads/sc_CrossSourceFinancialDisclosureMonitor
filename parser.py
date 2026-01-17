# drift/parser.py

def parse_normalized_terms(term: str):
    """
    Parses a normalized term string into a dictionary.
    Format: ENTITY|CLAIM_TYPE|SCOPE|KEY=VALUE
    Example: MSFT|EARNINGS|FY2024-Q2|EPS=2.93
    """
    if not term or not isinstance(term, str):
        raise ValueError("Input term must be a non-empty string")

    # Split into 4 parts max. 
    # This ensures that if the value contains '|', it stays part of the last segment.
    parts = term.split("|", 3)
    
    if len(parts) != 4:
        raise ValueError(f"Invalid term format. Expected 4 parts (ENTITY|TYPE|SCOPE|KV), got {len(parts)}: {term}")

    entity, claim_type, scope, kv = parts
    
    # Split key=value. Limit to 1 split to allow '=' in value (e.g. URLs).
    if "=" not in kv:
        raise ValueError(f"Invalid key-value format in '{kv}'. Expected 'KEY=VALUE'.")
        
    key, value = kv.split("=", 1)

    return {
        "entity": entity.strip(),
        "claim_type": claim_type.strip(),
        "scope": scope.strip(),
        "key": key.strip(),
        "value": value.strip()
    }
