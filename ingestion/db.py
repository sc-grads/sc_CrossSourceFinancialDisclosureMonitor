# ingestion/db.py

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, create_engine, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import ARRAY

# ---- load .env from project root ----
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

missing = [k for k, v in {
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD,
    "DB_HOST": DB_HOST,
    "DB_PORT": DB_PORT,
}.items() if not v]

if missing:
    raise ValueError(f"Missing environment variables: {', '.join(missing)}")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

Base = declarative_base()

# ---- tables ----
class Source(Base):
    __tablename__ = "sources"
    source_id = Column(Integer, primary_key=True)
    source_name = Column(String, unique=True, nullable=False)
    region = Column(String)
    status = Column(String, nullable=False, default="active")
    last_successful_fetch = Column(DateTime)

class Article(Base):
    __tablename__ = "articles"
    article_id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey("sources.source_id"), nullable=False)
    institution = Column(String)
    title = Column(Text)
    url = Column(Text)
    published_at = Column(DateTime)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class Claim(Base):
    __tablename__ = "claims"
    claim_id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey("articles.article_id"), nullable=False)
    normalized_terms = Column(Text, nullable=False)
    # ✅ new semantic vector storage
    embedding = Column(ARRAY(Float))
    extracted_at = Column(DateTime, default=datetime.utcnow)

# ---- engine/session ----
engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def init_db():
    Base.metadata.create_all(bind=engine)
