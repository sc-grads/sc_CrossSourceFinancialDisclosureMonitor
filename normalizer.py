# ingestion/normalizer.py
import re

STOPWORDS = {
    "the", "is", "a", "an", "to", "of", "and", "in", "on", "for", "with", "at", "by"
}

def normalize(text: str) -> str:
    text = (text or "").lower()
    tokens = re.findall(r"[a-z]+", text)
    tokens = [t for t in tokens if t not in STOPWORDS]
    tokens = sorted(set(tokens))
    return " ".join(tokens)
