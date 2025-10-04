import json, sqlite3, pandas as pd, plotly.express as px

def load_country_totals(sqlite_path):
    con = sqlite3.connect(sqlite_path)
    df = pd.read_sql_query("SELECT countries_json FROM gsf_minute_agg;", con)
    con.close()
    total = {}
    for j in df["countries_json"]:
        if not j: continue
        for k, v in json.loads(j).items():
            total[k] = total.get(k, 0) + v
    rows = [{"iso_alpha": k, "count": v} for k, v in total.items()]
    return pd.DataFrame(rows)

df = load_country_totals("data/gsf.sqlite")
fig = px.choropleth(df, locations="iso_alpha", color="count",
                    color_continuous_scale="Blues")
fig.update_layout(title="GSF Mentions by Country (last 24h)")
fig.show()
