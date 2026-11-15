"""Producer that streams live Wikipedia change events into Kafka.

It listens to the Wikimedia EventStreams SSE feed and publishes the changes of the
selected wikis (English Wikipedia and Wikidata by default) to the Kafka topic
configured in ``KAFKA_TOPIC``. Deciding which changes concern the IMDb dataset is
the job of the Spark pipeline (``src/streaming_pipeline.py``); an optional keyword
pre-filter can still be enabled with ``STREAM_KEYWORDS``.

The feed can be interrupted unexpectedly (timeouts, server restarts). The producer
therefore wraps the SSE consumption in a reconnection loop with exponential backoff,
so the pipeline keeps receiving events during the whole test window.

``--late-probe TITLE`` publishes one synthetic event (flagged ``_synthetic``) whose
event time lies ``--minutes`` in the past, to check that the pipeline's watermark
drops late data.
"""

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests
import sseclient
from kafka import KafkaProducer

WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki-changes")

DEFAULT_KEYWORDS = ("film", "movie", "actor", "actress", "cinema", "director")
DEFAULT_WIKIS = ("enwiki", "wikidatawiki")


def _env_list(name, default):
    value = os.getenv(name)
    if value is None:
        return tuple(default)
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


def user_agent(contact):
    """Wikimedia's User-Agent policy requires a real way to contact the operator."""
    if not contact or "@" not in contact:
        raise ValueError("Set WIKI_CONTACT to a real e-mail address of the group.")
    return f"ECE-BigData-IMDb-Streaming/1.0 (student project; {contact}) python-requests/{requests.__version__}"


def is_relevant(change, keywords=DEFAULT_KEYWORDS):
    """Return True if the change title or comment contains a relevant keyword."""
    text = f"{change.get('title', '')} {change.get('comment', '')}".lower()
    return any(keyword in text for keyword in keywords)


def should_forward(change, wikis=DEFAULT_WIKIS, keywords=()):
    """Keep the selected wikis; apply the keyword pre-filter only when keywords are given."""
    if wikis and change.get("wiki") not in wikis:
        return False
    return not keywords or is_relevant(change, keywords)


def build_producer():
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def stream_changes(producer, headers, wikis, keywords, log_every=500):
    """Consume the Wikimedia SSE feed once and forward the selected changes to Kafka."""
    response = requests.get(WIKI_STREAM_URL, stream=True, headers=headers, timeout=60)
    response.raise_for_status()
    client = sseclient.SSEClient(response)
    logging.info("Connected to Wikimedia EventStreams, publishing %s to %s", wikis or "all wikis", KAFKA_TOPIC)

    received = sent = 0
    for event in client.events():
        if event.event != "message":
            continue
        try:
            change = json.loads(event.data)
        except json.JSONDecodeError:
            continue
        received += 1
        if should_forward(change, wikis, keywords):
            change["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(KAFKA_TOPIC, change)
            sent += 1
            if sent % log_every == 0:
                logging.info("Forwarded %s of %s received events", sent, received)


def stream_changes_forever(producer, headers, wikis, keywords, max_backoff=60):
    """Reconnect automatically when the SSE feed drops.

    Uses exponential backoff capped at ``max_backoff`` seconds to avoid
    hammering the Wikimedia servers during an outage.
    """
    backoff = 1
    while True:
        try:
            stream_changes(producer, headers, wikis, keywords)
            backoff = 1  # reset after a clean run
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001 - reconnect on any network/SSE error
            logging.warning("Stream error: %s. Reconnecting in %ss...", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


def late_probe_event(title, minutes, wiki="enwiki", now=None):
    """A synthetic change whose event time is ``minutes`` before now."""
    now = now or datetime.now(timezone.utc)
    event_time = now - timedelta(minutes=minutes)
    return {
        "id": None, "type": "edit", "namespace": 0, "title": title, "wiki": wiki,
        "timestamp": int(event_time.timestamp()), "user": "late-probe", "comment": "synthetic late event",
        "meta": {"id": f"late-probe-{uuid.uuid4()}", "dt": event_time.isoformat()},
        "_ingested_at": now.isoformat(), "_synthetic": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--late-probe", metavar="TITLE",
                        help="publish one synthetic late event for this page title and exit")
    parser.add_argument("--minutes", type=float, default=30, help="age of the late probe (default 30)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = build_producer()
    try:
        if args.late_probe:
            event = late_probe_event(args.late_probe, args.minutes)
            producer.send(KAFKA_TOPIC, event)
            logging.info("Sent late probe %s (event time %s)", event["meta"]["id"], event["meta"]["dt"])
            return
        headers = {"User-Agent": user_agent(os.getenv("WIKI_CONTACT", "")), "Accept": "text/event-stream"}
        stream_changes_forever(producer, headers, _env_list("STREAM_WIKIS", DEFAULT_WIKIS),
                               _env_list("STREAM_KEYWORDS", ()))
    except KeyboardInterrupt:
        logging.info("Stopping producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
