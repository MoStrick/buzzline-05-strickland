"""
viz/choropleth_country_sentiment.py

Build a Plotly choropleth of average sentiment (VADER) by country,
using the minute-level aggregates the consumer writes to SQLite
(gsf_minute_agg: minute_iso, msg_count, mean_vader, emotions_json, countries_json).

Method:
- For each minute, we have mean_vader and a countries_json map like {"USA": 12, "GBR": 4}
- We compute per-country sentiment contribution for that minute as:
    sentiment_sum[country] += mean_vader * country_count
    message_count[country]  += country_count
- Then country_mean_sentiment = sentiment_sum[country] / message_count[country]

This is equivalent to averaging the per-message VADER scores, assuming the minute's mean
is the average of all messages in that minute (which the consumer computed).

Output:
- Saves an interactive HTML at images/gsf_choropleth.html
- Optionally opens it in a browser (set OPEN_IN_BROWSER=True)
"""

import json
import sqlite3
import pathlib
import webbrowser
from typing import Dict

import pandas as pd
import plotly.express as px

# Local config (reuse your utils_config so paths match .env)
try:
    import utils.utils_config as config
except Exception:
    config = None


OPEN_IN_BROWSER = True  # set False if running on a headless machine


def load_minute_table(sqlite_path: pathlib.Path) -> pd.DataFrame:
    con = sqlite3.connect(str(sqlite_path))
    try:
        df = pd.read_sql_query(
            "SELECT minute_iso, msg_count, mean_vader, countries_json FROM gsf_minute_agg;",
            con,
        )
    finally:
        con.close()
    return df


def compute_country_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """
    From minute-level rows, compute per-country:
      - total_msgs
      - sentiment_sum (mean_vader * msgs in that country for that minute)
      - mean_sentiment = sentiment_sum / total_msgs
    Returns a DataFrame with columns: iso_alpha, total_msgs, mean_sentiment
    """
    sentiment_sum: Dict[str, float] = {}
    total_msgs: Dict[str, int] = {}

    for _, row in df.iterrows():
        mean_vader = float(row.get("mean_vader") or 0.0)
        countries_json = row.get("countries_json") or "{}"

        try:
            countries = json.loads(countries_json)
            # countries is like {"USA": 12, "GBR": 4, ...}
            for iso3, count in countries.items():
                c = int(count or 0)
                if c <= 0:
                    continue
                sentiment_sum[iso3] = sentiment_sum.get(iso3, 0.0) + mean_vader * c
                total_msgs[iso3] = total_msgs.get(iso3, 0) + c
        except Exception:
            # skip malformed rows
            continue

    rows = []
    for iso3, n in total_msgs.items():
        if n <= 0:
            continue
        mean_sent = sentiment_sum.get(iso3, 0.0) / n
        rows.append({"iso_alpha": iso3, "total_msgs": n, "mean_sentiment": mean_sent})

    return pd.DataFrame(rows)


def make_choropleth(country_df: pd.DataFrame, out_html: pathlib.Path) -> None:
    """
    Build a diverging choropleth centered at 0 for sentiment.
    Hover shows message counts and sentiment value.
    """
    if country_df.empty:
        print("No country data available yet. Run the pipeline to generate aggregates.")
        return

    # Clip extreme values (optional) to keep color scale readable
    country_df["mean_sentiment"] = country_df["mean_sentiment"].clip(-1, 1)

    fig = px.choropleth(
        country_df,
        locations="iso_alpha",           # ISO-3 country codes
        color="mean_sentiment",          # average VADER per country
        hover_name="iso_alpha",
        hover_data={"total_msgs": True, "mean_sentiment": True, "iso_alpha": False},
        color_continuous_scale="RdBu",   # divergent: red(-) ↔ blue(+)
        range_color=(-1, 1),
        title="Global Sumud Flotilla — Average Sentiment by Country (last 24h)",
    )
    fig.update_layout(
        coloraxis_colorbar=dict(title="Sentiment (VADER)", ticksuffix=""),
        margin=dict(l=10, r=10, t=60, b=10),
    )

    out_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(out_html), include_plotlyjs="cdn")
    print(f"Choropleth saved to: {out_html}")

    if OPEN_IN_BROWSER:
        webbrowser.open(out_html.as_uri())


def main():
    # Resolve SQLite path from your config (preferred), else fallback
    if config and hasattr(config, "get_sqlite_path"):
        sqlite_path = config.get_sqlite_path()
    else:
        sqlite_path = pathlib.Path("data/gsf.sqlite")

    out_html = pathlib.Path("images/gsf_choropleth.html").resolve()

    df = load_minute_table(sqlite_path)
    country_df = compute_country_sentiment(df)
    make_choropleth(country_df, out_html)


if __name__ == "__main__":
    main()
