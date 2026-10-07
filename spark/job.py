"""
Spark Structured Streaming job.

Kafka topic (raw Wikimedia events)
  -> parse JSON, keep only real edits
  -> 1-minute windows per (wiki, is_bot)
  -> upsert into PostgreSQL table edits_per_minute
"""
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-edits")

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=5432,
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    dbname=os.environ["POSTGRES_DB"],
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS edits_per_minute (
    window_start  TIMESTAMPTZ NOT NULL,
    wiki          TEXT        NOT NULL,
    is_bot        BOOLEAN     NOT NULL,
    edit_count    INT         NOT NULL,
    bytes_changed BIGINT      NOT NULL,
    PRIMARY KEY (window_start, wiki, is_bot)
);
"""

UPSERT_SQL = """
INSERT INTO edits_per_minute
    (window_start, wiki, is_bot, edit_count, bytes_changed)
VALUES %s
ON CONFLICT (window_start, wiki, is_bot) DO UPDATE SET
    edit_count    = EXCLUDED.edit_count,
    bytes_changed = EXCLUDED.bytes_changed;
"""

# Only the fields we need. Spark ignores the rest of the JSON.
EVENT_SCHEMA = StructType([
    StructField("type", StringType()),
    StructField("wiki", StringType()),
    StructField("bot", BooleanType()),
    StructField("timestamp", LongType()),          # unix seconds
    StructField("length", StructType([
        StructField("old", LongType()),
        StructField("new", LongType()),
    ])),
])


def ensure_table():
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
    finally:
        conn.close()


def upsert_batch(batch_df, batch_id):
    """
    Called once per micro-batch. In 'update' output mode each row is the
    latest TOTAL for that window, so we overwrite (not add to) the old row.
    """
    rows = [
        (r.window_start, r.wiki, r.is_bot, r.edit_count, r.bytes_changed)
        for r in batch_df.collect()   # small: a few rows per wiki per minute
    ]
    if not rows:
        return
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows)
    finally:
        conn.close()
    print(f"batch {batch_id}: upserted {len(rows)} rows", flush=True)


def main():
    ensure_table()

    spark = (
        SparkSession.builder.appName("wikipulse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")   # default 200 is overkill locally
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")       # replay what's already in Kafka
        .option("maxOffsetsPerTrigger", 20000)       # cap batch size
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .filter(F.col("type") == "edit")             # drop categorize, log, new, ...
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .withColumn("is_bot", F.coalesce(F.col("bot"), F.lit(False)))
        .withColumn(
            "bytes_changed",
            F.coalesce(F.col("length.new"), F.lit(0))
            - F.coalesce(F.col("length.old"), F.lit(0)),
        )
        .filter(F.col("wiki").isNotNull() & F.col("event_time").isNotNull())
    )

    per_minute = (
        events
        .withWatermark("event_time", "2 minutes")    # wait up to 2 min for late events
        .groupBy(F.window("event_time", "1 minute"), "wiki", "is_bot")
        .agg(
            F.count("*").alias("edit_count"),
            F.sum("bytes_changed").alias("bytes_changed"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "wiki", "is_bot", "edit_count", "bytes_changed",
        )
    )

    query = (
        per_minute.writeStream
        .outputMode("update")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", "/checkpoints/edits_per_minute")
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
