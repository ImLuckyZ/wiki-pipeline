"""WikiPulse API: reads the aggregates Spark wrote and serves them to the dashboard."""
import os

import psycopg2
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from psycopg2.extras import RealDictCursor

app = FastAPI(title="WikiPulse API")

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    dbname=os.environ["POSTGRES_DB"],
)

# Reused in every query: lets the dashboard toggle Wikidata on/off
WIKIDATA_FILTER = "(%(inc)s OR wiki <> 'wikidatawiki')"


def run(sql, params):
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/api/edits-per-minute")
def edits_per_minute(include_wikidata: bool = False, minutes: int = 60):
    return run(f"""
        SELECT window_start, SUM(edit_count) AS edits
        FROM edits_per_minute
        WHERE window_start >= now() - make_interval(mins => %(m)s)
          AND {WIKIDATA_FILTER}
        GROUP BY window_start ORDER BY window_start
    """, {"m": minutes, "inc": include_wikidata})


@app.get("/api/top-wikis")
def top_wikis(include_wikidata: bool = False):
    return run(f"""
        SELECT wiki, total_edits, RANK() OVER (ORDER BY total_edits DESC) AS rnk
        FROM (
            SELECT wiki, SUM(edit_count) AS total_edits
            FROM edits_per_minute
            WHERE window_start >= now() - interval '10 minutes'
              AND {WIKIDATA_FILTER}
            GROUP BY wiki
        ) t
        ORDER BY rnk LIMIT 10
    """, {"inc": include_wikidata})


@app.get("/api/bot-share")
def bot_share(include_wikidata: bool = False):
    rows = run(f"""
        SELECT COALESCE(SUM(edit_count) FILTER (WHERE is_bot), 0)     AS bot,
               COALESCE(SUM(edit_count) FILTER (WHERE NOT is_bot), 0) AS human
        FROM edits_per_minute
        WHERE window_start >= now() - interval '60 minutes'
          AND {WIKIDATA_FILTER}
    """, {"inc": include_wikidata})
    return rows[0]


# Serve the dashboard at "/" (must come after the API routes)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
