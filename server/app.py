from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
from datetime import datetime

from ingestion.search import search_with_auto_ingest
from sentence_transformers import SentenceTransformer

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Load embedding model once (cosine similarity via dot product because embeddings are normalized)
_EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")


def _embed(text: str):
    return _EMBED_MODEL.encode(text or "", normalize_embeddings=True)


def _cos_sim(a, b) -> float:
    # embeddings normalized => cosine similarity = dot product
    return float((a * b).sum())


@app.route("/receive", methods=["POST", "OPTIONS"])
def receive():
    data = request.get_json(silent=True) or {}

    # Support both lowercase and uppercase keys
    title = str(data.get("title") or data.get("Title") or "").strip()

    # Ignore blank titles
    if not title:
        return jsonify({"status": "ignored", "reason": "blank title"})

    record = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "title": title,
        "url": data.get("url") or data.get("URL"),
        "text_length": data.get("text_length") or data.get("Text length"),
        "preview": data.get("preview") or data.get("Preview"),
    }

    # Print as formatted JSON (for logs)
    print(json.dumps(record, indent=2))

    try:
        # Run semantic search (+ auto-ingest if needed)
        result = search_with_auto_ingest(title, min_similarity=0.75)

        # Confidence from support ratio
        support_ratio = result.get("support_ratio")
        confidence_percent = None
        if support_ratio is not None:
            confidence_percent = int(round(float(support_ratio) * 100))

        # Evidence / sources
        evidence = result.get("evidence") or []

        # Compute per-source similarity + average similarity across ALL returned sources
        user_vec = _embed(title)

        sims = []
        sources = []
        for e in evidence:
            ev_title = e.get("title") or ""
            ev_vec = _embed(ev_title)
            sim = _cos_sim(user_vec, ev_vec)  # 0..1
            sims.append(sim)

        avg_similarity_percent = None
        if sims:
            avg_similarity_percent = int(round((sum(sims) / len(sims)) * 100))

        # For UI: return top 6 cards (but avg computed over all evidence above)
        for e in evidence[:6]:
            ev_title = e.get("title") or ""
            ev_vec = _embed(ev_title)
            sim = _cos_sim(user_vec, ev_vec)
            sim_pct = int(round(sim * 100))

            sources.append({
                "source_name": e.get("source_name"),
                "institution": e.get("institution"),
                "title": e.get("title"),
                "url": e.get("url"),
                "published_at": e.get("published_at"),
                "similarity_percent": sim_pct,
            })

        return jsonify({
            "status": "ok",
            "input": record,
            "result": result,  # raw result
            "confidence_percent": confidence_percent,
            "avg_similarity_percent": avg_similarity_percent,
            "sources": sources,
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "input": record,
            "error": str(e),
        }), 500


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
