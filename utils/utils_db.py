# utils/utils_db.py
import sqlite3
from pathlib import Path

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=3000;

CREATE TABLE IF NOT EXISTS minute_emotions (
  minute_utc TEXT PRIMARY KEY,
  msg_count INTEGER,
  mean_vader REAL,
  anger REAL, fear REAL, anticipation REAL, trust REAL,
  surprise REAL, sadness REAL, joy REAL, disgust REAL
);

CREATE TABLE IF NOT EXISTS minute_country_stats (
  minute_utc TEXT,
  country_code TEXT,          -- use ISO-3 for easy choropleth joins
  link_count INTEGER,
  mean_vader REAL,
  anger REAL, fear REAL, anticipation REAL, trust REAL,
  surprise REAL, sadness REAL, joy REAL, disgust REAL,
  PRIMARY KEY (minute_utc, country_code)
);

CREATE INDEX IF NOT EXISTS idx_mcs_minute ON minute_country_stats(minute_utc);
CREATE INDEX IF NOT EXISTS idx_mcs_country ON minute_country_stats(country_code);
"""

def get_conn(db_path="data/gsf_minute_agg.db"):
    Path("data").mkdir(exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn

def init_db(conn):
    conn.executescript(SCHEMA_SQL)
    conn.commit()
