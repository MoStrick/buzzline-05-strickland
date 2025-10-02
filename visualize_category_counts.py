# visualize_category_counts.py
import os, sqlite3
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()

DB_PATH = os.getenv("SQLITE_DB_PATH", "data/insights.db")

def fetch_counts(conn):
    cur = conn.execute("""
        SELECT category, total_msgs
        FROM category_counts
        ORDER BY total_msgs DESC, category ASC
    """)
    return cur.fetchall()

def main(save_png: bool = True):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    rows = fetch_counts(conn)
    conn.close()

    if not rows:
        print("No data yet. Let your consumer run for a bit, then try again.")
        return

    cats = [r[0] for r in rows]
    vals = [r[1] for r in rows]

    plt.figure()
    plt.bar(cats, vals)
    plt.title("r/learnmath — Category Distribution")
    plt.xlabel("Category")
    plt.ylabel("Total Messages")
    plt.tight_layout()

    if save_png:
        out = "data/category_counts.png"
        os.makedirs(os.path.dirname(out), exist_ok=True)
        plt.savefig(out, dpi=160)
        print(f"Saved: {out}")

    plt.show()

if __name__ == "__main__":
    main()
