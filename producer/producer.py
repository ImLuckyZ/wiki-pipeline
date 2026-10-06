"""
Reads the live Wikimedia "recentchange" stream (Server-Sent Events)
and publishes every event to a Kafka topic.
"""
import json
import os
import time

import requests
from confluent_kafka import Producer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-edits")

# Wikimedia asks clients to identify themselves with contact info.
# We read it from the environment (set in .env) so it never lives in the code.
CONTACT = os.getenv("WIKI_CONTACT")
if not CONTACT:
    raise SystemExit("WIKI_CONTACT is not set. Add it to your .env file.")

HEADERS = {
    "User-Agent": f"wiki-pipeline-portfolio/0.1 ({CONTACT})",
    "Accept": "text/event-stream",
}

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 50,          # wait up to 50ms to batch messages (more efficient)
    "compression.type": "lz4",
})


def on_delivery(err, msg):
    """Called by Kafka client after each message is sent (or fails)."""
    if err is not None:
        print(f"Delivery failed: {err}", flush=True)


def stream_events(last_event_id=None):
    """Yield (event_id, raw_json_string) for each event on the stream."""
    headers = dict(HEADERS)
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id  # resume where we left off

    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        event_id = None
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            if line.startswith("id:"):
                event_id = line[3:].strip()
            elif line.startswith("data:"):
                yield event_id, line[5:].strip()


def main():
    last_id = None
    sent = 0
    last_report = time.time()
    backoff = 1

    print(f"Producing to {BOOTSTRAP}, topic '{TOPIC}'", flush=True)

    while True:
        try:
            for event_id, raw in stream_events(last_id):
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue  # skip malformed lines

                # Key by wiki: events from the same wiki land in the same partition
                producer.produce(
                    TOPIC,
                    key=event.get("wiki", "unknown"),
                    value=raw,
                    callback=on_delivery,
                )
                producer.poll(0)  # serve delivery callbacks

                last_id = event_id or last_id
                sent += 1
                backoff = 1  # healthy connection, reset retry delay

                if time.time() - last_report >= 10:
                    print(f"Sent {sent} events so far", flush=True)
                    last_report = time.time()

        except requests.RequestException as e:
            print(f"Stream error: {e}. Reconnecting in {backoff}s", flush=True)
            if getattr(e, "response", None) is not None and e.response.status_code == 400:
                last_id = None  # saved position was rejected, start fresh
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except KeyboardInterrupt:
            break

    producer.flush()


if __name__ == "__main__":
    main()
