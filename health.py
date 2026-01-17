from datetime import datetime

def mark_active(db, source):
    source.status = "active"
    source.last_successful_fetch = datetime.utcnow()
    db.commit()

def mark_offline(db, source):
    source.status = "offline"
    db.commit()
