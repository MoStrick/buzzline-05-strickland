# tools/visualize_category_counts.py
import os, sqlite3
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()
DB_PATH = os.getenv("SQLITE_DB_PATH", "data/insights.db")
OUT_PATH = "data/category_counts.png"

def fetch_counts(conn):
    return conn.execute("""
        SELECT category, total_msgs
        FROM category_counts
        ORDER BY total_msgs DESC, category ASC
    """).fetchall()

def main():
    print(f"Reading DB from: {DB_PATH}")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    rows = fetch_counts(conn)
    conn.close()

    if not rows:
        print("No data yet. Let the consumer write some rows, then try again.")
        # Still show an empty figure so you see *something*
        plt.figure()
        plt.title("No data yet")
        plt.tight_layout()
        plt.show()
        return

    cats = [r[0] for r in rows]
    vals = [r[1] for r in rows]

    plt.figure()
    plt.bar(cats, vals)
    plt.title("Category Distribution")
    plt.xlabel("Category")
    plt.ylabel("Total Messages")
    plt.tight_layout()

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    plt.savefig(OUT_PATH, dpi=160)
    print(f"Saved chart: {OUT_PATH}")
    plt.show()

if __name__ == "__main__":
    main()
