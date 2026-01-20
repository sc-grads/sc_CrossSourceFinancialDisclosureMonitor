# ingestion/health.py
from datetime import datetime
 
def mark_active(db, source, reason: str | None = None):
    source.status = "active"
    source.last_successful_fetch = datetime.utcnow()
 
    # Optional column support (safe if not present)
    if hasattr(source, "last_error"):
        source.last_error = None
 
    db.commit()
 
def mark_offline(db, source, reason: str | None = None):
    source.status = "offline"
 
    # Optional column support (safe if not present)
    if hasattr(source, "last_error") and reason:
        source.last_error = str(reason)[:500]
 
    db.commit()