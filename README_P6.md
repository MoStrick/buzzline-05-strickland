Project 6
├─ .env
├─ requirements.txt
├─ data/                    # created at runtime
├─ logs/                    # created at runtime
├─ producers/
│  ├─ reddit_gsf_producer.py         # (you already have from last message)
│  └─ outlet_country_map.py          # NEW: curated outlet→country lookup
├─ utils/
│  ├─ utils_config.py                # NEW: reads env vars safely (one place)
│  ├─ utils_logger.py                # NEW: sets up project logger + file logs
│  ├─ utils_producer.py              # NEW: verify services, create topic
│  ├─ utils_country.py               # NEW: outlet map + TLD fallback (NER optional)
│  └─ emitters/
│     ├─ __init__.py
│     ├─ file_emitter.py             # NEW: write JSONL lines
│     ├─ kafka_emitter.py            # NEW: send to Kafka
│     ├─ sqlite_emitter.py           # NEW: persist to SQLite
│     └─ duckdb_emitter.py           # NEW: persist to DuckDB
