# tools/live_category_counts.py
import os, time, sqlite3
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()
DB_PATH = os.getenv("SQLITE_DB_PATH", "data/insights.db")
REFRESH_SECS = int(os.getenv("REFRESH_SECS", "5"))

def fetch(conn):
    return conn.execute("""
        SELECT category, total_msgs
        FROM category_counts
        ORDER BY total_msgs DESC, category ASC
    """).fetchall()

def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    plt.ion()
    fig = plt.figure()
    ax = fig.gca()

    while True:
        rows = fetch(conn)
        ax.clear()
        if rows:
            cats = [r[0] for r in rows]
            vals = [r[1] for r in rows]
            ax.bar(cats, vals)
            ax.set_title("r/learnmath — Category Distribution (Live)")
            ax.set_xlabel("Category")
            ax.set_ylabel("Total Messages")
            ax.set_ylim(0, max(vals) * 1.15 if vals else 1)
        else:
            ax.set_title("No data yet… waiting for consumer to write rows")
            ax.set_xlabel("")
            ax.set_ylabel("")
        fig.tight_layout()
        fig.canvas.draw()
        fig.canvas.flush_events()
        time.sleep(REFRESH_SECS)

if __name__ == "__main__":
    main()

