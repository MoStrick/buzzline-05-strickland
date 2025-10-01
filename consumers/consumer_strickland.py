"""
consumer_molly.py

Live Reddit stream (subreddit: r/learnmath by default) -> SQLite category distribution.
Processes ONE comment at a time: classify into a math category and update counts.

Run:
    py -m consumers.consumer_molly
"""

import os
import re
import sqlite3
from datetime import datetime
from typing import Dict

from dotenv import load_dotenv
import praw

load_dotenv()

# --- ENV ---
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USERNAME      = os.getenv("REDDIT_USERNAME")
REDDIT_PASSWORD      = os.getenv("REDDIT_PASSWORD")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "learnmath-category-consumer")
SUBREDDIT            = os.getenv("REDDIT_SUBREDDIT", "learnmath")

DB_PATH              = os.getenv("SQLITE_DB_PATH", "data/insights.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# --- Category rules (keyword -> category). Tweak any time. ---
CATEGORY_PATTERNS: Dict[str, str] = {
    r"\balgebra|linear equation|quadratic|factor\b": "algebra",
    r"\bcalculus|derivative|integral|limit|series|epsilon|delta\b": "calculus",
    r"\bgeometry|triangle|circle|polygon|congruence|similar\b": "geometry",
    r"\bstat(istics|s)?|probability|expectation|variance|distribution\b": "statistics",
    r"\bprecalculus|trigonometry|trig|vector|matrix\b": "precalculus",
}
DEFAULT_CATEGORY = "general"

def classify_category(text: str) -> str:
    t = text.lower()
    for pattern, category in CATEGORY_PATTERNS.items():
        if re.search(pattern, t):
            return category
    return DEFAULT_CATEGORY

# --- SQLite helpers ---
DDL_COUNTS = """
CREATE TABLE IF NOT EXISTS category_counts (
    category TEXT PRIMARY KEY,
    total_msgs INTEGER NOT NULL DEFAULT 0,
    last_ts TEXT
);
"""
DDL_MESSAGES = """
CREATE TABLE IF NOT EXISTS messages_processed (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    author TEXT,
    category TEXT,
    snippet TEXT
);
"""

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    with conn:
        conn.execute(DDL_COUNTS)
        conn.execute(DDL_MESSAGES)

def upsert_category(conn: sqlite3.Connection, category: str, ts: str) -> None:
    cur = conn.execute("SELECT total_msgs FROM category_counts WHERE category = ?", (category,))
    row = cur.fetchone()
    if row is None:
        conn.execute("INSERT INTO category_counts(category, total_msgs, last_ts) VALUES(?, ?, ?)",
                     (category, 1, ts))
    else:
        conn.execute("UPDATE category_counts SET total_msgs = total_msgs + 1, last_ts = ? WHERE category = ?",
                     (ts, category))

def insert_message(conn: sqlite3.Connection, ts: str, author: str, category: str, snippet: str) -> None:
    conn.execute(
        "INSERT INTO messages_processed(ts, author, category, snippet) VALUES (?, ?, ?, ?)",
        (ts, author[:80], category, snippet[:300])
    )

def build_reddit() -> praw.Reddit:
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
        raise RuntimeError("Missing client_id/client_secret/user_agent in .env")
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        check_for_async=False,
    )


def process_stream():
    conn = get_conn()
    init_db(conn)

    reddit = build_reddit()
    sub = reddit.subreddit(SUBREDDIT)

    print(f"[consumer_molly] Streaming comments from r/{SUBREDDIT} → {DB_PATH}")
    # skip_existing=True starts from *new* comments only
    for comment in sub.stream.comments(skip_existing=True):
        try:
            text = comment.body or ""
            category = classify_category(text)
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            author = str(comment.author) if comment.author else "unknown"

            # ONE comment at a time → update counts + audit row
            upsert_category(conn, category, ts)
            insert_message(conn, ts, author, category, text)

            conn.commit()
            print(f"[ok] r/{SUBREDDIT} | {author:>12} → {category}")
        except Exception as e:
            print("[warn] error processing a comment:", e)
            conn.rollback()

def main():
    process_stream()

if __name__ == "__main__":
    main()
