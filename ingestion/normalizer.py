# ingestion/normalizer.py
import re

STOPWORDS = {
    "the", "is", "a", "an", "to", "of", "and", "in", "on", "for", "with", "at", "by"
}

# Words that flip the meaning of a claim
NEGATION_WORDS = {
    "not", "no", "never", "unlikely", "without", "avoid", "fail"
}

def normalize(text: str) -> str:
    """
    Converts text into a deterministic, comparable token string.
    Example:
    'Tesla earnings not expected to surge'
    -> 'earnings expected not surge tesla'
    """
    text = (text or "").lower()
    tokens = re.findall(r"[a-z]+", text)
    tokens = [t for t in tokens if t not in STOPWORDS]
    tokens = sorted(set(tokens))
    return " ".join(tokens)

def has_negation(text: str) -> bool:
    """
    Detects whether a sentence contains negation.
    This is NOT NLP — just a deterministic rule.
    """
    text = (text or "").lower()
    words = set(re.findall(r"[a-z]+", text))
    return not words.isdisjoint(NEGATION_WORDS)
