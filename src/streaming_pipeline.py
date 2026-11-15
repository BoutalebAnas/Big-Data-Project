"""Spark Structured Streaming pipeline for Wikimedia change events.

Reads the ``wiki-changes`` Kafka topic and keeps the changes that concern the IMDb
dataset. Matching is done in Spark with a stream-static join against a reference
table built from the prepared IMDb snapshot and the enrichment outputs:

* ``wikidata_item``  - Wikidata edits on the items of enriched films and principals;
* ``wikipedia_page`` - English Wikipedia edits on the articles of those films/people;
* ``imdb_title``     - English Wikipedia articles titled ``<title> (<year> film)``
  whose base title is an IMDb movie released between 1980 and 2025.

Late data: event time is the Wikimedia ``timestamp``. A watermark bounds the state of
the two stateful operators (de-duplication of matched events and event-time window
counts); rows older than the watermark are dropped by those operators and reported in
the ``numRowsDroppedByWatermark`` metric recorded for every micro-batch.
"""

import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki-changes")
STARTING_OFFSETS = os.getenv("STREAM_STARTING_OFFSETS", "earliest")
WATERMARK_DELAY = os.getenv("STREAM_WATERMARK", "10 minutes")
WINDOW_DURATION = os.getenv("STREAM_WINDOW", "5 minutes")
TRIGGER = os.getenv("STREAM_TRIGGER", "30 seconds")
FIRST_YEAR, LAST_YEAR = 1980, 2025

WIKIPEDIA_ARTICLE = r"^https://en\.wikipedia\.org/wiki/(.+)$"
FILM_ARTICLE = r"^(.+) \((?:[0-9]{4} )?(?:[a-z-]+ )?film\)$"

SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("namespace", LongType()),
    StructField("title", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("comment", StringType()),
    StructField("wiki", StringType()),
    StructField("server_name", StringType()),
    StructField("meta", StructType([
        StructField("id", StringType()),
        StructField("dt", StringType()),
        StructField("domain", StringType()),
    ])),
    StructField("_ingested_at", StringType()),
    StructField("_synthetic", BooleanType()),
])


def kafka_package():
    """Kafka connector matching the installed PySpark version (override with SPARK_KAFKA_PACKAGE)."""
    import pyspark

    return os.getenv("SPARK_KAFKA_PACKAGE",
                     f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}")


def build_spark():
    return (
        SparkSession.builder.appName("WikiStreamingPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", kafka_package())
        .getOrCreate()
    )


def parse_events(df):
    """Parse the raw Kafka payload into a structured DataFrame.

    Events whose JSON payload cannot be parsed are dropped by filtering on a
    required field (``title``). The Kafka record timestamp is kept when present.
    """
    has_kafka_time = "timestamp" in df.columns
    extra = [F.col("timestamp").alias("kafka_timestamp")] if has_kafka_time else []
    return (
        df.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("data"), *extra)
        .select("data.*", *(["kafka_timestamp"] if has_kafka_time else []))
        .filter(F.col("title").isNotNull())
    )


def with_event_time(df):
    """Event time from the Unix timestamp, plus ingestion time and delay when available."""
    df = df.withColumn("event_time", F.to_timestamp(F.col("timestamp").cast("long")))
    if "_ingested_at" in df.columns:
        df = df.withColumn("ingested_at", F.to_timestamp("_ingested_at")).withColumn(
            "ingest_delay_s", F.col("ingested_at").cast("double") - F.col("event_time").cast("double")
        )
    return df


def normalize_page(column):
    """Lower-cased page title without the talk prefix and with spaces instead of underscores."""
    title = F.regexp_replace(F.regexp_replace(column, "_", " "), r"^(?i)talk:", "")
    return F.lower(F.trim(title))


def film_base_title(column):
    """``inside out`` for ``Inside Out (2015 film)``; empty string otherwise."""
    return F.regexp_extract(normalize_page(column), FILM_ARTICLE, 1)


def _page_from_url(column):
    encoded = F.regexp_extract(column, WIKIPEDIA_ARTICLE, 1)
    return F.when(encoded != "", normalize_page(F.call_function("url_decode", encoded)))


def build_reference(*, films=None, principals=None, titles=None, first_year=FIRST_YEAR, last_year=LAST_YEAR):
    """Static lookup table ``(match_type, match_key) -> imdb_ids, label``.

    ``films`` and ``principals`` are the enrichment outputs (``tconst``/``nconst``,
    ``primaryTitle``/``primaryName``, ``source_url``, ``wikidata_uri``); ``titles`` is the
    prepared ``title.basics`` table. Each argument is optional.
    """
    parts = []
    for frame, key, label in ((films, "tconst", "primaryTitle"), (principals, "nconst", "primaryName")):
        if frame is None:
            continue
        entities = frame.select(F.col(key).alias("imdb_id"), F.col(label).alias("label"),
                                "source_url", "wikidata_uri").distinct()
        parts.append(entities.select(
            F.lit("wikidata_item").alias("match_type"),
            F.regexp_extract("wikidata_uri", r"(Q[0-9]+)$", 1).alias("match_key"), "imdb_id", "label",
        ))
        parts.append(entities.select(
            F.lit("wikipedia_page").alias("match_type"), _page_from_url("source_url").alias("match_key"),
            "imdb_id", "label",
        ))
    if titles is not None:
        movies = titles.filter((F.col("titleType") == "movie")
                               & F.col("startYear").between(first_year, last_year))
        parts.append(movies.select(
            F.lit("imdb_title").alias("match_type"), F.lower(F.trim("primaryTitle")).alias("match_key"),
            F.col("tconst").alias("imdb_id"), F.col("primaryTitle").alias("label"),
        ))
    if not parts:
        raise ValueError("The reference needs at least one of films, principals or titles.")
    reference = parts[0]
    for part in parts[1:]:
        reference = reference.unionByName(part)
    return (
        reference.filter(F.col("match_key").isNotNull() & (F.col("match_key") != ""))
        .groupBy("match_type", "match_key")
        .agg(F.array_sort(F.collect_set("imdb_id")).alias("imdb_ids"), F.min("label").alias("label"))
    )


def match_events(events, reference):
    """Stream-static join: one row per (event, matching reference key)."""
    title = F.col("title")
    key = lambda kind, value: F.struct(F.lit(kind).alias("match_type"), value.alias("match_key"))  # noqa: E731
    base = film_base_title(title)
    candidates = F.array(
        F.when((F.col("wiki") == "wikidatawiki") & title.rlike(r"^Q[0-9]+$"), key("wikidata_item", title)),
        F.when(F.col("wiki") == "enwiki", key("wikipedia_page", normalize_page(title))),
        F.when((F.col("wiki") == "enwiki") & (base != ""), key("imdb_title", base)),
    )
    uid = F.coalesce(F.col("meta.id"), F.concat_ws(":", "wiki", F.col("id").cast("string"),
                                                  F.col("timestamp").cast("string"), "title"))
    keyed = (
        events.withColumn("event_uid", uid)
        .withColumn("_key", F.explode(candidates)).filter(F.col("_key").isNotNull())
        .withColumn("match_type", F.col("_key.match_type"))
        .withColumn("match_key", F.col("_key.match_key")).drop("_key")
    )
    return keyed.join(F.broadcast(reference), ["match_type", "match_key"])


def deduplicate(matched):
    """Drop replays of the same event (producer reconnections) within the watermark.

    Batch DataFrames (tests, offline re-processing) fall back to a plain de-duplication.
    """
    keys = ["event_uid", "match_type"]
    return matched.dropDuplicatesWithinWatermark(keys) if matched.isStreaming else matched.dropDuplicates(keys)


def window_counts(matched, window=WINDOW_DURATION):
    """Matched events per event-time window; final once the watermark passes the window end."""
    aggregates = [F.count("*").alias("events"), F.approx_count_distinct("match_key").alias("distinct_items")]
    if "ingest_delay_s" in matched.columns:
        aggregates.append(F.avg("ingest_delay_s").alias("mean_ingest_delay_s"))
    counts = matched.groupBy(F.window("event_time", window).alias("window"), "match_type").agg(*aggregates)
    return counts.select(F.col("window.start").alias("window_start"),
                         F.col("window.end").alias("window_end"), "*").drop("window")


class ProgressRecorder(StreamingQueryListener):
    """Append every micro-batch progress report (JSON) to a file kept with the outputs."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _write(self, record):
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record) + "\n")

    def onQueryStarted(self, event):
        self._write({"event": "started", "id": str(event.id), "name": event.name, "timestamp": event.timestamp})

    def onQueryProgress(self, event):
        self._write({"event": "progress", "progress": json.loads(event.progress.json)})

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        self._write({"event": "terminated", "id": str(event.id), "exception": event.exception})


def _paths():
    from src.imdb_loader import data_root

    root = data_root(Path(__file__).resolve().parents[1])
    base = Path(os.getenv("STREAM_OUTPUT", (root / "streaming" / "output").as_posix()))
    checkpoints = Path(os.getenv("STREAM_CHECKPOINT", (root / "streaming" / "checkpoints").as_posix()))
    return root, base, checkpoints


def load_reference(spark, root):
    """Reference from the prepared snapshot and, when present, its enrichment outputs."""
    from src.imdb_loader import prepared_snapshot

    snapshot = prepared_snapshot(root)
    titles = spark.read.parquet((snapshot / "tables" / "title.basics").as_posix())
    enriched = root / "enrichment" / "output" / snapshot.name
    films = principals = None
    if (enriched / "films").is_dir() and (enriched / "principals").is_dir():
        films = spark.read.parquet((enriched / "films").as_posix())
        principals = spark.read.parquet((enriched / "principals").as_posix())
    else:
        print(f"No enrichment output in {enriched}: matching on IMDb film titles only. "
              "Run 03_data_enrichment.ipynb for Wikidata/Wikipedia matches.")
    return build_reference(films=films, principals=principals, titles=titles)


def ensure_topic(broker=KAFKA_BROKER, topic=KAFKA_TOPIC):
    """Create the topic when missing: the Kafka source fails if it starts before the producer."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        if topic not in admin.list_topics():
            admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
            print(f"Created Kafka topic {topic}")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def run_pipeline(spark):
    """Build and start the two streaming queries, then wait (optionally for a fixed duration)."""
    ensure_topic()
    root, base, checkpoints = _paths()
    spark.streams.addListener(ProgressRecorder(base / "metrics" / "progress.jsonl"))
    reference = load_reference(spark, root).cache()
    print("Reference keys by type:")
    reference.groupBy("match_type").count().show(truncate=False)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    events = with_event_time(parse_events(raw)).withWatermark("event_time", WATERMARK_DELAY)
    matched = deduplicate(match_events(events, reference))

    queries = [
        matched.writeStream.queryName("matched_events").format("parquet")
        .option("path", (base / "matched_events").as_posix())
        .option("checkpointLocation", (checkpoints / "matched_events").as_posix())
        .outputMode("append").trigger(processingTime=TRIGGER).start(),
        window_counts(matched).writeStream.queryName("window_counts").format("parquet")
        .option("path", (base / "window_counts").as_posix())
        .option("checkpointLocation", (checkpoints / "window_counts").as_posix())
        .outputMode("append").trigger(processingTime=TRIGGER).start(),
    ]
    minutes = os.getenv("STREAM_DURATION_MINUTES")
    try:
        if minutes:
            spark.streams.awaitAnyTermination(float(minutes) * 60)
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
