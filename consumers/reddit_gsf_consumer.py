"""
reddit_gsf_consumer.py

Purpose
-------
Consume JSON messages produced by reddit_gsf_producer.py, keep a 24-hour rolling
window of per-minute aggregates (counts, mean VADER, NRC emotions, countries),
optionally persist minute-level aggregates to SQLite, and render live charts.

Why consumers follow this structure
-----------------------------------
1) Read configuration   -> connect to Kafka topic (or dev fallback file).
2) Deserialize messages -> validate schema and normalize.
3) Update aggregates    -> O(1) dict/Counter updates for the current minute.
4) Persist (optional)   -> checkpoint minute bins to SQLite on an interval.
5) Visualize            -> live charts updated on a timer (matplotlib).

Key ideas in this consumer
--------------------------
- 24h rolling window via deque(maxlen=1440) + dict of minute bins
- VADER sentiment (fast, built-in lexicon via NLTK)
- NRC emotions (optional, via NRCLex) with a safe fallback if not installed
- Country counts (from message['country'] already attached by producer)
- SQLite minute aggregates: minute ISO, msg_count, mean_vader, emotions_json, countries_json

Install (if needed)
-------------------
pip install kafka-python-ng matplotlib nltk nrclex sqlite-utils
# If you want the optional world map:
pip install plotly pandas

First run only (for VADER):
python -c "import nltk; nltk.download('vader_lexicon')"
"""

#####################################
# Imports
#####################################

# stdlib
import json
import os
import sys
import time
import pathlib
import signal
from collections import deque, Counter, defaultdict
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

# external (Kafka)
try:
    from kafka import KafkaConsumer  # kafka-python-ng
    KAFKA_AVAILABLE = True
except ImportError:
    KafkaConsumer = None
    KAFKA_AVAILABLE = False

# external (analytics)
import matplotlib.pyplot as plt

# VADER sentiment (fast; classic for social text)
from nltk.sentiment import SentimentIntensityAnalyzer

# NRC emotions (optional; we fail soft if not installed)
try:
    from nrclex import NRCLex
    NRC_AVAILABLE = True
except Exception:
    NRC_AVAILABLE = False

# external (SQLite)
import sqlite3

# local utils
from utils.utils_logger import logger
import utils.utils_config as config


#####################################
# Configuration / Constants
#####################################

# 24h rolling window -> 1440 minutes
WINDOW_MINUTES = 1440

# Chart refresh cadence (seconds)
REDRAW_EVERY_SECS = 2

# Persist to SQLite every N seconds
SQLITE_FLUSH_EVERY_SECS = 30

# Which emotions do we track (NRC has more; pick a consistent subset for clarity)
EMOTION_KEYS = ["anger", "anticipation", "disgust", "fear", "joy",
                "sadness", "surprise", "trust"]


#####################################
# Utilities
#####################################

def minute_key_utc(ts: Optional[datetime] = None) -> str:
    """Return the UTC minute key as ISO string YYYY-MM-DDTHH:MM."""
    if ts is None:
        ts = datetime.now(timezone.utc)
    ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M")


def parse_message(raw: bytes) -> Optional[Dict[str, Any]]:
    """Decode a Kafka bytes message into a Python dict; return None on failure."""
    try:
        obj = json.loads(raw.decode("utf-8"))
        # minimal schema sanity
        if not isinstance(obj, dict):
            return None
        return obj
    except Exception:
        return None


#####################################
# Sentiment / Emotion
#####################################

class SentimentEmotion:
    """
    Wrap VADER and (optionally) NRC into a simple interface.
    - VADER -> compound score in [-1, 1].
    - NRC -> returns list of emotion labels present in the text.
    """

    def __init__(self) -> None:
        try:
            self.vader = SentimentIntensityAnalyzer()
        except Exception as e:
            logger.error("VADER not available. Did you download vader_lexicon?")
            raise

        self.nrc_on = NRC_AVAILABLE
        if self.nrc_on:
            logger.info("NRCLex available: NRC emotions will be computed.")
        else:
            logger.info("NRCLex NOT available: emotions will be empty (fallback).")

    def vader_score(self, text: str) -> float:
        return float(self.vader.polarity_scores(text or "")["compound"])

    def emotions(self, text: str) -> List[str]:
        if not self.nrc_on:
            return []
        try:
            # NRCLex processes raw text and has .top_emotions or .raw_emotion_scores
            n = NRCLex(text or "")
            # Convert raw_emotion_scores dict -> list of emotions present
            raw = n.raw_emotion_scores  # e.g., {'fear': 2, 'trust': 1}
            # Keep only our EMOTION_KEYS
            return [e for e in EMOTION_KEYS if raw.get(e, 0) > 0]
        except Exception:
            return []


#####################################
# Aggregator: 24h rolling minute bins
#####################################

class MinuteAggregator:
    """
    Maintain per-minute aggregates in memory and optionally persist to SQLite.

    Structure:
      window: deque of minute keys (maxlen=1440)
      bins: dict minute_key -> {
          "msg_count": int,
          "sum_vader": float,
          "emotions": Counter(),
          "countries": Counter()
      }
    """

    def __init__(self, max_minutes: int = WINDOW_MINUTES) -> None:
        self.window = deque(maxlen=max_minutes)
        self.bins: Dict[str, Dict[str, Any]] = {}

    def ensure_minute(self, k: str) -> None:
        if k not in self.bins:
            self.bins[k] = {
                "msg_count": 0,
                "sum_vader": 0.0,
                "emotions": Counter(),
                "countries": Counter(),
            }
            self.window.append(k)
            # prune bins if deque evicted an old key
            while len(self.window) > self.window.maxlen:
                old = self.window.popleft()
                self.bins.pop(old, None)

    def update(self, *, ts_utc_iso_minute: str, vader: float,
               emo_labels: List[str], country: Optional[str]) -> None:
        self.ensure_minute(ts_utc_iso_minute)
        b = self.bins[ts_utc_iso_minute]
        b["msg_count"] += 1
        b["sum_vader"] += vader
        for e in emo_labels:
            if e in EMOTION_KEYS:
                b["emotions"][e] += 1
        if country:
            b["countries"][country] += 1

    def mean_vader(self, k: str) -> float:
        b = self.bins.get(k)
        if not b:
            return 0.0
        return (b["sum_vader"] / b["msg_count"]) if b["msg_count"] else 0.0

    def series_for_plot(self) -> Tuple[List[str], List[int], List[float],
                                       Dict[str, List[int]], List[Tuple[str, int]]]:
        """
        Return aligned series for charts:
        - minutes (sorted ascending)
        - msg_counts per minute
        - mean_vader per minute
        - emotions_by_key: dict emotion -> list aligned counts
        - top_countries: list of (country, total_count) sorted desc (top 10)
        """
        minutes = sorted(self.bins.keys())
        msg_counts = [self.bins[m]["msg_count"] for m in minutes]
        mean_vaders = [self.mean_vader(m) for m in minutes]

        emotions_by_key: Dict[str, List[int]] = {e: [] for e in EMOTION_KEYS}
        for m in minutes:
            emo_counter = self.bins[m]["emotions"]
            for e in EMOTION_KEYS:
                emotions_by_key[e].append(emo_counter.get(e, 0))

        # aggregate countries across the whole window for the bar chart
        total_countries = Counter()
        for m in minutes:
            total_countries.update(self.bins[m]["countries"])
        top_countries = total_countries.most_common(10)

        return minutes, msg_counts, mean_vaders, emotions_by_key, top_countries


#####################################
# SQLite persistence (minute-level)
#####################################

DDL = """
CREATE TABLE IF NOT EXISTS gsf_minute_agg (
    minute_iso TEXT PRIMARY KEY,
    msg_count INTEGER,
    mean_vader REAL,
    emotions_json TEXT,
    countries_json TEXT
);
"""

UPSERT = """
INSERT INTO gsf_minute_agg (minute_iso, msg_count, mean_vader, emotions_json, countries_json)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(minute_iso) DO UPDATE SET
  msg_count=excluded.msg_count,
  mean_vader=excluded.mean_vader,
  emotions_json=excluded.emotions_json,
  countries_json=excluded.countries_json;
"""

def flush_minute_bins_to_sqlite(agg: MinuteAggregator, db_path: pathlib.Path) -> None:
    """Upsert all current minute bins into SQLite."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(DDL)
        for m in sorted(agg.bins.keys()):
            b = agg.bins[m]
            msg_count = b["msg_count"]
            mean_vader = (b["sum_vader"] / msg_count) if msg_count else 0.0
            emotions_json = json.dumps(dict(b["emotions"]))
            countries_json = json.dumps(dict(b["countries"]))
            conn.execute(UPSERT, (m, msg_count, mean_vader, emotions_json, countries_json))
        conn.commit()
    finally:
        conn.close()


#####################################
# Visualization (matplotlib)
#####################################

class LiveDashboard:
    """
    Create and update simple live charts:
      - messages per minute
      - mean VADER per minute
      - emotions (multiple lines)
      - top countries (bar)
    """

    def __init__(self):
        # Turn on interactive mode so plt.pause works
        plt.ion()
        self.fig = plt.figure(figsize=(14, 9))
        self.ax_rate = self.fig.add_subplot(2, 2, 1)
        self.ax_vader = self.fig.add_subplot(2, 2, 2)
        self.ax_emotions = self.fig.add_subplot(2, 2, 3)
        self.ax_countries = self.fig.add_subplot(2, 2, 4)
        self.fig.suptitle("GSF Streaming Dashboard (24h rolling window)", fontsize=14)

    def redraw(self, agg: MinuteAggregator):
        minutes, msg_counts, mean_vaders, emotions_by_key, top_countries = agg.series_for_plot()

        # Rate chart
        self.ax_rate.clear()
        self.ax_rate.set_title("Messages per Minute (last 24h)")
        if minutes:
            self.ax_rate.plot(minutes, msg_counts)
            self.ax_rate.set_xticks(minutes[::max(1, len(minutes)//6)])
            self.ax_rate.tick_params(axis='x', rotation=30)
        self.ax_rate.set_ylabel("count")

        # VADER chart
        self.ax_vader.clear()
        self.ax_vader.set_title("Mean VADER per Minute (last 24h)")
        if minutes:
            self.ax_vader.plot(minutes, mean_vaders)
            self.ax_vader.set_xticks(minutes[::max(1, len(minutes)//6)])
            self.ax_vader.tick_params(axis='x', rotation=30)
        self.ax_vader.set_ylabel("compound [-1,1]")

        # Emotions chart
        self.ax_emotions.clear()
        self.ax_emotions.set_title("Top Emotions per Minute (lines)")
        if minutes:
            for e in EMOTION_KEYS:
                self.ax_emotions.plot(minutes, emotions_by_key[e], label=e)
            self.ax_emotions.set_xticks(minutes[::max(1, len(minutes)//6)])
            self.ax_emotions.tick_params(axis='x', rotation=30)
            self.ax_emotions.legend(loc="upper left", ncol=2, fontsize=8)
        self.ax_emotions.set_ylabel("count")

        # Countries bar
        self.ax_countries.clear()
        self.ax_countries.set_title("Top Countries (aggregated over window)")
        if top_countries:
            labels = [c for c, _ in top_countries]
            values = [v for _, v in top_countries]
            self.ax_countries.bar(labels, values)
        self.ax_countries.set_ylabel("count")

        self.fig.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.pause(0.001)  # allow UI to update


#####################################
# Kafka consumer (soft fallback to file)
#####################################

def create_kafka_consumer() -> Optional[KafkaConsumer]:
    """
    Create a KafkaConsumer from env config, or return None if unavailable.
    """
    if not KAFKA_AVAILABLE:
        logger.info("Kafka library not installed; consumer will read from file fallback.")
        return None

    try:
        broker = config.get_kafka_broker_address()
        topic = config.get_kafka_topic()
        group_id = config.get_kafka_consumer_group_id()
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            group_id=group_id,
            auto_offset_reset="latest",   # start from latest to see current stream
            enable_auto_commit=True,
            consumer_timeout_ms=1000,     # short poll to allow UI refresh
        )
        logger.info(f"KafkaConsumer connected to {broker}, topic={topic}, group={group_id}")
        return consumer
    except Exception as e:
        logger.warning(f"KafkaConsumer setup failed: {e}")
        return None


#####################################
# Fallback: tail a JSONL file (producer file sink)
#####################################

def follow_file(path: pathlib.Path):
    """
    Generator that tails a JSONL file, yielding bytes lines to mimic Kafka messages.
    """
    logger.info(f"Following file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as f:
        f.seek(0, os.SEEK_END)
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                time.sleep(0.5)
                f.seek(pos)
                continue
            yield line.encode("utf-8")


#####################################
# Main loop
#####################################

def main() -> None:
    logger.info("Starting Reddit GSF Consumer. Ctrl+C to stop.")

    # Config
    sqlite_path = config.get_sqlite_path()
    live_file = config.get_live_data_path()  # same as producer's JSONL sink
    redraw_secs = REDRAW_EVERY_SECS

    # Analytics helpers
    sentemo = SentimentEmotion()
    agg = MinuteAggregator(max_minutes=WINDOW_MINUTES)
    dash = LiveDashboard()

    # Input stream: Kafka -> preferred; else follow JSONL file
    consumer = create_kafka_consumer()
    use_kafka = consumer is not None
    if not use_kafka:
        logger.info("Using file-follow fallback for input stream.")

    last_sqlite_flush = time.time()
    last_redraw = 0.0

    # Graceful exit on Ctrl+C
    stop = False
    def _sigint(_sig, _frm):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _sigint)

    try:
        while not stop:
            if use_kafka:
                # Poll Kafka briefly then refresh UI
                batch_count = 0
                for msg in consumer:
                    if msg is None:
                        break
                    obj = parse_message(msg.value)
                    if not obj:
                        continue

                    # Extract fields from producer schema
                    text = obj.get("text", "")
                    country = obj.get("country")  # "GBR", "USA", "UNK", etc.
                    # Use message utc_ts minute if present; else now
                    try:
                        ts_key = (obj.get("utc_ts") or "")
                        # keep only minute precision if given
                        ts_key = ts_key[:16] if len(ts_key) >= 16 else minute_key_utc()
                    except Exception:
                        ts_key = minute_key_utc()

                    # Compute or reuse VADER (producer did not compute it)
                    vader = sentemo.vader_score(text)
                    emo_labels = sentemo.emotions(text)

                    agg.update(ts_utc_iso_minute=ts_key, vader=vader,
                               emo_labels=emo_labels, country=country)

                    batch_count += 1
                    if batch_count >= 500:
                        break  # let UI redraw
            else:
                # File-follow fallback
                for raw in follow_file(live_file):
                    obj = parse_message(raw)
                    if not obj:
                        continue
                    text = obj.get("text", "")
                    country = obj.get("country")
                    try:
                        ts_key = (obj.get("utc_ts") or "")
                        ts_key = ts_key[:16] if len(ts_key) >= 16 else minute_key_utc()
                    except Exception:
                        ts_key = minute_key_utc()
                    vader = sentemo.vader_score(text)
                    emo_labels = sentemo.emotions(text)
                    agg.update(ts_utc_iso_minute=ts_key, vader=vader,
                               emo_labels=emo_labels, country=country)
                    # throttle a bit so UI can breathe
                    break

            # Periodic SQLite flush
            if time.time() - last_sqlite_flush > SQLITE_FLUSH_EVERY_SECS:
                try:
                    flush_minute_bins_to_sqlite(agg, sqlite_path)
                    logger.info("Flushed aggregates to SQLite.")
                except Exception as e:
                    logger.warning(f"SQLite flush failed: {e}")
                last_sqlite_flush = time.time()

            # Periodic redraw
            if time.time() - last_redraw > redraw_secs:
                dash.redraw(agg)
                last_redraw = time.time()

            # Small sleep so we don’t peg a CPU core
            time.sleep(0.2)

    except Exception as e:
        logger.error(f"Consumer crashed: {e}")
    finally:
        try:
            if consumer:
                consumer.close()
        except Exception:
            pass
        logger.info("Consumer shutting down.")
        # keep figures open for inspection when stopping manually
        try:
            plt.ioff()
            plt.show()
        except Exception:
            pass


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
