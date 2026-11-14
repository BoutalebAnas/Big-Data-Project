"""Producer that streams live Wikipedia change events into Kafka.

It listens to the Wikimedia EventStreams SSE feed, keeps only events whose
title or comment mention movie-related keywords, and publishes them to the
Kafka topic configured in ``KAFKA_TOPIC``.

The feed can be interrupted unexpectedly (timeouts, server restarts). The
producer therefore wraps the SSE consumption in a reconnection loop with
exponential backoff, so the pipeline keeps receiving events during the whole
test window.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
import sseclient
from kafka import KafkaProducer

WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki-changes")
USER_AGENT = "ECE-BigData-Project/1.0 (student@ece.fr)"

DEFAULT_KEYWORDS = ("film", "movie", "actor", "actress", "cinema", "director")


def is_relevant(change, keywords=DEFAULT_KEYWORDS):
    """Return True if the change title or comment contains a relevant keyword."""
    text = f"{change.get('title', '')} {change.get('comment', '')}".lower()
    return any(keyword in text for keyword in keywords)


def build_producer():
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def stream_changes(producer, keywords=DEFAULT_KEYWORDS):
    """Consume the Wikimedia SSE feed once and forward relevant changes to Kafka."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    response = requests.get(WIKI_STREAM_URL, stream=True, headers=headers, timeout=60)
    response.raise_for_status()
    client = sseclient.SSEClient(response)
    logging.info("Connected to Wikimedia EventStreams, publishing to %s", KAFKA_TOPIC)

    for event in client.events():
        if event.event != "message":
            continue
        try:
            change = json.loads(event.data)
        except json.JSONDecodeError:
            continue
        if is_relevant(change, keywords):
            change["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(KAFKA_TOPIC, change)
            logging.info("Sent: %s (%s)", change.get("title"), change.get("type"))


def stream_changes_forever(producer, keywords=DEFAULT_KEYWORDS, max_backoff=60):
    """Reconnect automatically when the SSE feed drops.

    Uses exponential backoff capped at ``max_backoff`` seconds to avoid
    hammering the Wikimedia servers during an outage.
    """
    backoff = 1
    while True:
        try:
            stream_changes(producer, keywords)
            backoff = 1  # reset after a clean run
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001 - reconnect on any network/SSE error
            logging.warning("Stream error: %s. Reconnecting in %ss...", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = build_producer()
    try:
        stream_changes_forever(producer)
    except KeyboardInterrupt:
        logging.info("Stopping producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()