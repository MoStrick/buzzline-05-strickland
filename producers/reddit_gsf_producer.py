"""
reddit_gsf_producer.py

Purpose
-------
Stream Reddit posts/comments that match "Global Sumud Flotilla" keywords,
enrich with URL domains and (best-effort) country tags, and emit JSON to any
combination of sinks (File, Kafka, SQLite, DuckDB) using the same emitter
pattern as your base producer.

Why producers follow this structure
-----------------------------------
1) Read configuration   -> so behavior is adjustable without editing code.
2) Connect to source    -> Reddit API client with auth.
3) Generate messages    -> loop that yields normalized JSON dicts.
4) Emit to sinks        -> single-responsibility emitters per sink.
5) Be resilient         -> logging, soft-fail on optional services, cleanup.

Environment variables are read from utils/utils_config (same as your project).
This script also uses:
- tldextract (to parse domains robustly)
- praw (Reddit API)
- utils_country.get_country_from_domain (your outlet map + TLD/NER fallback)

Install (if needed)
-------------------
pip install praw tldextract
"""

#####################################
# Import Modules
#####################################

# stdlib
import json
import os
import re
import sys
import time
import pathlib
from datetime import datetime, timezone
from typing import Mapping, Any, Iterable, Dict, List, Optional

# external
import tldextract
try:
    from kafka import KafkaProducer  # kafka-python-ng
except ImportError:
    KafkaProducer = None  # Optional; we soft-handle Kafka being unavailable.

import praw  # Reddit API client

# local (your project utilities)
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger
from utils.emitters import file_emitter, kafka_emitter, sqlite_emitter, duckdb_emitter

# country helper (uses outlet_country_map internally)
from utils.utils_country import get_country_from_domain


#####################################
# Project-Specific Settings
#####################################

# 1) Keywords that define the Global Sumud Flotilla
FLOTILLA_KEYWORDS: List[str] = [
    "flotilla",
    "sumud flotilla",
    "gaza flotilla",
    "gaza aid flotilla",
    "global sumud",
    "freedom flotilla",
]

# 2) Subreddits to monitor (start small; expand as needed)
SUBREDDITS: List[str] = [
    "worldnews",
    "news",
    "politics",
    "MiddleEastNews",
    "israel",
    "palestine",
]

# 3) Simple URL extractor (matches http/https links in text)
URL_REGEX = re.compile(r"https?://\S+")


#####################################
# Small Helpers
#####################################

def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO format (with 'Z')."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def extract_urls(text: str) -> List[str]:
    """Find all http(s) URLs inside a text block."""
    return URL_REGEX.findall(text or "")


def extract_domains(urls: Iterable[str]) -> List[str]:
    """Robustly extract registrable domains (e.g., 'bbc.co.uk') from URLs."""
    domains = []
    for u in urls:
        try:
            ext = tldextract.extract(u)
            # ext.domain + ext.suffix -> 'bbc' + 'co.uk' -> 'bbc.co.uk'
            if ext.domain and ext.suffix:
                domains.append(f"{ext.domain}.{ext.suffix}")
        except Exception:
            # ignore malformed URLs
            pass
    return domains


def text_matches_keywords(text: str, keywords: List[str]) -> bool:
    """Case-insensitive keyword OR-match for filtering."""
    text_lower = (text or "").lower()
    return any(k.lower() in text_lower for k in keywords)


#####################################
# Single-Responsibility Emitters (per sink)
#####################################

def emit_to_file(message: Mapping[str, Any], *, path: pathlib.Path) -> bool:
    """Append one message to a JSONL file."""
    return file_emitter.emit_message(message, path=path)


def emit_to_kafka(message: Mapping[str, Any], *, producer, topic: str) -> bool:
    """Publish one message to a Kafka topic (if Kafka connected)."""
    if producer is None:
        return False
    return kafka_emitter.emit_message(message, producer=producer, topic=topic)


def emit_to_sqlite(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """Insert one message into SQLite."""
    return sqlite_emitter.emit_message(message, db_path=db_path)


def emit_to_duckdb(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """Insert one message into DuckDB."""
    return duckdb_emitter.emit_message(message, db_path=db_path)


#####################################
# Reddit Client Setup
#####################################

def create_reddit_client() -> praw.Reddit:
    """
    Create an authenticated Reddit client.

    Why producers do this:
    - Keep auth & API setup in one place.
    - Read credentials from config so we don't hard-code secrets.

    Required config getters (add in utils/utils_config if missing):
        get_reddit_client_id()
        get_reddit_client_secret()
        get_reddit_user_agent()
    """
    try:
        client_id = config.get_reddit_client_id()
        client_secret = config.get_reddit_client_secret()
        user_agent = config.get_reddit_user_agent()
    except Exception as e:
        logger.error(f"Failed to read Reddit API env vars: {e}")
        raise

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        # read-only use case; no username/password needed
        check_for_async=False,
    )
    return reddit


#####################################
# Message Normalization
#####################################

def normalize_submission(sub) -> Dict[str, Any]:
    """Turn a PRAW Submission into our normalized JSON message."""
    text = f"{sub.title}\n\n{sub.selftext or ''}".strip()
    urls = list(set(extract_urls(text) + ([sub.url] if sub.url else [])))
    url_domains = extract_domains(urls)

    # Country enrichment: use first domain as hint; pass text for NER fallback.
    country = "UNK"
    if url_domains:
        country = get_country_from_domain(url_domains[0], text=text) or "UNK"

    return {
        "source": "submission",
        "id": f"t3_{sub.id}",                       # Reddit thing_id style
        "utc_ts": utc_now_iso(),
        "subreddit": str(sub.subreddit),
        "permalink": f"https://www.reddit.com{sub.permalink}",
        "author": str(getattr(sub, "author", "")) if sub.author else None,
        "text": text,
        "urls": urls,
        "url_domains": url_domains,
        "country": country,
        "score": int(getattr(sub, "score", 0)),
        "num_comments": int(getattr(sub, "num_comments", 0)),
    }


def normalize_comment(cmt) -> Dict[str, Any]:
    """Turn a PRAW Comment into our normalized JSON message."""
    text = getattr(cmt, "body", "") or ""
    urls = extract_urls(text)
    url_domains = extract_domains(urls)

    country = "UNK"
    if url_domains:
        country = get_country_from_domain(url_domains[0], text=text) or "UNK"

    return {
        "source": "comment",
        "id": f"t1_{cmt.id}",
        "utc_ts": utc_now_iso(),
        "subreddit": str(cmt.subreddit),
        "permalink": f"https://www.reddit.com{cmt.permalink}",
        "author": str(getattr(cmt, "author", "")) if cmt.author else None,
        "text": text,
        "urls": urls,
        "url_domains": url_domains,
        "country": country,
        "score": int(getattr(cmt, "score", 0)),
    }


#####################################
# Generator: Stream Reddit
#####################################

def generate_reddit_events(reddit: praw.Reddit, *, pause_secs: int = 2) -> Dict[str, Any]:
    """
    Yield normalized messages from submissions and comments that match keywords.

    Why producers do this:
    - Use a generator that "yields forever" to drive the emit loop.
    - Keep filtering & normalization here, so main loop stays clean.

    Streaming approach:
    - For each subreddit, use .stream.submissions() and .stream.comments()
      with pause_after to make polling non-blocking.
    - Filter by keywords to keep only relevant events.
    """
    subs = "+".join(SUBREDDITS)
    subreddit = reddit.subreddit(subs)

    submissions_stream = subreddit.stream.submissions(pause_after=0)
    comments_stream = subreddit.stream.comments(pause_after=0)

    while True:
        # Submissions
        try:
            for sub in submissions_stream:
                if sub is None:  # no new items; stream yields None occasionally
                    break
                text_block = f"{sub.title}\n{sub.selftext or ''}"
                if text_matches_keywords(text_block, FLOTILLA_KEYWORDS):
                    yield normalize_submission(sub)
        except Exception as e:
            logger.warning(f"Submission stream error (continuing): {e}")

        # Comments
        try:
            for cmt in comments_stream:
                if cmt is None:
                    break
                text_block = getattr(cmt, "body", "") or ""
                if text_matches_keywords(text_block, FLOTILLA_KEYWORDS):
                    yield normalize_comment(cmt)
        except Exception as e:
            logger.warning(f"Comment stream error (continuing): {e}")

        time.sleep(pause_secs)  # small backoff to be polite to the API


#####################################
# Main
#####################################

def main() -> None:
    logger.info("Starting Reddit GSF Producer (Ctrl+C to stop).")

    # STEP 1. Read config (interval controls *emit* pacing to sinks; stream itself is event-driven)
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()                # e.g., "gsf_event_stream"
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()  # e.g., data/live.jsonl
        sqlite_path: pathlib.Path = getattr(config, "get_sqlite_path", lambda: pathlib.Path("data/gsf.sqlite"))()
        duckdb_path: pathlib.Path = getattr(config, "get_duckdb_path", lambda: pathlib.Path("data/gsf.duckdb"))()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # STEP 2. Prep file sink (fresh file each run)
    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info(f"Deleted existing file: {live_data_path}")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to prep file sink: {e}")
        sys.exit(2)

    # STEP 3. Reddit client
    try:
        reddit = create_reddit_client()
        logger.info("Reddit client ready (read-only).")
    except Exception as e:
        logger.error(f"ERROR: Reddit client setup failed: {e}")
        sys.exit(3)

    # STEP 4. Optional Kafka setup (soft-fail)
    producer = None
    try:
        if KafkaProducer and verify_services(strict=False):
            producer = KafkaProducer(bootstrap_servers=kafka_server)
            logger.info(f"Kafka producer connected to {kafka_server}")
            try:
                create_kafka_topic(topic)
                logger.info(f"Kafka topic '{topic}' is ready.")
            except Exception as e:
                logger.warning(f"Topic verify/create warning for '{topic}': {e}")
        else:
            logger.info("Kafka disabled or kafka-python not installed.")
    except Exception as e:
        logger.warning(f"Kafka setup failed (continuing without Kafka): {e}")
        producer = None

    # STEP 5. Emit loop: for each Reddit event, call the sinks you want; toggle kafka, SQLite, and DuckDB by commenting out the code)
    try:
        for message in generate_reddit_events(reddit, pause_secs=2):
            logger.info(message)

            # --- File (JSONL) ---
            emit_to_file(message, path=live_data_path)

            # --- Kafka ---
            if producer is not None:
                emit_to_kafka(message, producer=producer, topic=topic)

            # --- SQLite --- (optional: uncomment to enable)
            # emit_to_sqlite(message, db_path=sqlite_path)

            # --- DuckDB --- (optional: uncomment to enable)
            # emit_to_duckdb(message, db_path=duckdb_path)

            # pacing for sinks (does NOT control the Reddit polling rate)
            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected producer error: {e}")
    finally:
        if producer:
            try:
                producer.flush(timeout=5)
                producer.close()
                logger.info("Kafka producer closed.")
            except Exception:
                pass
        logger.info("Reddit GSF Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
