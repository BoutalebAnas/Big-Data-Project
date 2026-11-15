"""Unit tests for the streaming producer and pipeline logic.

These tests do not require Kafka or Docker: they only exercise pure
transformation functions on local Spark DataFrames.
"""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from src import streaming_pipeline as sp
from src.streaming_pipeline import parse_events, with_event_time
from src.streaming_producer import is_relevant, late_probe_event, should_forward, user_agent


def test_is_relevant_matches_title_and_comment():
    """The keyword filter must match title OR comment, case-insensitively."""
    assert is_relevant({"title": "The Matrix", "comment": "new movie"})
    assert is_relevant({"title": "Film industry", "comment": ""})
    assert is_relevant({"title": "Some page", "comment": "added an actor"})
    assert not is_relevant({"title": "Football", "comment": "score update"})


def test_parse_events_ignores_invalid_json(spark):
    """Invalid JSON payloads must be dropped, not emitted as null rows."""
    raw = spark.createDataFrame(
        [
            (b'{"title":"A","type":"edit","timestamp":1700000000}',),
            (b'not json',),
            (b'{"title":"B","type":"new","timestamp":1700000100}',),
        ],
        "value binary",
    )
    parsed = parse_events(raw).collect()
    assert {row.title for row in parsed} == {"A", "B"}


def test_with_event_time_converts_timestamp(spark):
    """Unix timestamps must be converted to a Spark timestamp column."""
    raw = spark.createDataFrame(
        [(1700000000,)],
        "timestamp long",
    )
    result = with_event_time(raw).first()
    expected = dt.datetime.fromtimestamp(1700000000, tz=dt.timezone.utc).year
    assert result.event_time.year == expected

# --- Producer routing -------------------------------------------------------------------------


def test_should_forward_selects_wikis_and_optional_keywords():
    assert should_forward({"wiki": "enwiki", "title": "Football"})
    assert should_forward({"wiki": "wikidatawiki", "title": "Q42"})
    assert not should_forward({"wiki": "frwiki", "title": "Film"})
    assert should_forward({"wiki": "frwiki", "title": "x"}, wikis=())
    assert not should_forward({"wiki": "enwiki", "title": "Football"}, keywords=("film",))


def test_user_agent_requires_a_real_contact():
    assert "team@example.org" in user_agent("team@example.org")
    with pytest.raises(ValueError):
        user_agent("")


def test_late_probe_is_synthetic_and_in_the_past():
    now = dt.datetime(2026, 11, 15, 12, 0, tzinfo=dt.timezone.utc)
    event = late_probe_event("Inside Out (2015 film)", 30, now=now)
    assert event["_synthetic"] and event["meta"]["id"].startswith("late-probe-")
    assert event["timestamp"] == int((now - dt.timedelta(minutes=30)).timestamp())


# --- Spark matching ---------------------------------------------------------------------------

EVENT_COLUMNS = "wiki string, title string, timestamp long, id long, meta struct<id:string>"


def test_titles_are_normalized_for_matching(spark):
    rows = spark.createDataFrame(
        [("Talk:Inside_Out_(2015_film)",), ("Heat (1995 American film)",), ("Inside Out",), ("Film",)],
        "title string",
    ).select(sp.normalize_page(F.col("title")).alias("page"), sp.film_base_title(F.col("title")).alias("base"))
    assert [(r.page, r.base) for r in rows.collect()] == [
        ("inside out (2015 film)", "inside out"), ("heat (1995 american film)", "heat"),
        ("inside out", ""), ("film", ""),
    ]


def reference(spark):
    films = spark.createDataFrame([
        ("tt1", "Inside Out", "https://en.wikipedia.org/wiki/Inside_Out_(2015_film)",
         "http://www.wikidata.org/entity/Q6144664"),
    ], "tconst string, primaryTitle string, source_url string, wikidata_uri string")
    principals = spark.createDataFrame([
        ("nm1", "Amélie Doe", "https://en.wikipedia.org/wiki/Am%C3%A9lie_Doe", None),
    ], "nconst string, primaryName string, source_url string, wikidata_uri string")
    titles = spark.createDataFrame([
        ("tt1", "movie", "Inside Out", 2015), ("tt2", "movie", "Inside Out", 2011),
        ("tt3", "movie", "Old Film", 1950), ("tt4", "tvSeries", "Heat", 2000),
    ], "tconst string, titleType string, primaryTitle string, startYear int")
    return sp.build_reference(films=films, principals=principals, titles=titles)


def test_reference_combines_enrichment_and_imdb_titles(spark):
    rows = {(r.match_type, r.match_key): r.imdb_ids for r in reference(spark).collect()}
    assert rows == {
        ("wikidata_item", "Q6144664"): ["tt1"],
        ("wikipedia_page", "inside out (2015 film)"): ["tt1"],
        ("wikipedia_page", "amélie doe"): ["nm1"],
        ("imdb_title", "inside out"): ["tt1", "tt2"],             # 1950 film and series excluded
    }


def test_events_match_reference_and_duplicates_are_dropped(spark):
    events = spark.createDataFrame([
        ("enwiki", "Inside Out (2015 film)", 1700000000, 1, ("a",)),   # page + film title
        ("enwiki", "Inside Out (2015 film)", 1700000000, 1, ("a",)),   # replayed duplicate
        ("wikidatawiki", "Q6144664", 1700000010, 2, ("b",)),
        ("enwiki", "Amélie_Doe", 1700000020, 3, ("c",)),
        ("frwiki", "Inside Out (2015 film)", 1700000030, 4, ("d",)),   # other wiki: ignored
        ("enwiki", "Football", 1700000040, 5, ("e",)),
    ], EVENT_COLUMNS)
    events = sp.with_event_time(events).withWatermark("event_time", "10 minutes")
    matched = sp.deduplicate(sp.match_events(events, reference(spark)))
    rows = sorted((r.event_uid, r.match_type, tuple(r.imdb_ids)) for r in matched.collect())
    assert rows == [
        ("a", "imdb_title", ("tt1", "tt2")), ("a", "wikipedia_page", ("tt1",)),
        ("b", "wikidata_item", ("tt1",)), ("c", "wikipedia_page", ("nm1",)),
    ]
    counts = {(r.match_type, r.events) for r in sp.window_counts(matched, "5 minutes").collect()}
    assert counts == {("imdb_title", 1), ("wikipedia_page", 2), ("wikidata_item", 1)}


def test_parse_events_keeps_kafka_timestamp_and_ingestion_delay(spark):
    raw = spark.createDataFrame([
        (b'{"title":"A","timestamp":1700000000,"_ingested_at":"2023-11-14T22:13:25+00:00"}',
         dt.datetime(2023, 11, 14, 22, 13, 30)),
    ], "value binary, timestamp timestamp")
    row = sp.with_event_time(sp.parse_events(raw)).first()
    assert row.kafka_timestamp is not None and row.ingest_delay_s == pytest.approx(5.0)


def test_streaming_watermark_drops_late_events(spark, tmp_path):
    """End-to-end micro-batches: an event older than the watermark is discarded."""
    import json

    source = tmp_path / "events"
    source.mkdir()
    schema = sp.SCHEMA

    def publish(batch, rows):
        lines = [json.dumps({"wiki": "enwiki", "title": title, "timestamp": ts, "meta": {"id": uid}})
                 for uid, title, ts in rows]
        (source / f"batch-{batch}.json").write_text("\n".join(lines) + "\n", encoding="utf-8")

    stream = spark.readStream.schema(schema).json(source.as_posix())
    events = sp.with_event_time(stream).withWatermark("event_time", "10 minutes")
    matched = sp.deduplicate(sp.match_events(events, reference(spark)))
    query = (matched.writeStream.format("memory").queryName("late_test").outputMode("append")
             .option("checkpointLocation", (tmp_path / "checkpoint").as_posix()).start())
    try:
        t0 = 1700000000
        publish(1, [("on-time", "Inside Out (2015 film)", t0)])
        query.processAllAvailable()
        publish(2, [("advance", "Inside Out (2015 film)", t0 + 3600)])   # moves the watermark
        query.processAllAvailable()
        publish(3, [("late", "Inside Out (2015 film)", t0 + 60),          # 59 min behind
                    ("advance", "Inside Out (2015 film)", t0 + 3600)])    # replayed duplicate
        query.processAllAvailable()
        progress = query.recentProgress
    finally:
        query.stop()
    rows = sorted((row.event_uid, row.match_type) for row in spark.table("late_test").collect())
    assert rows == [("advance", "imdb_title"), ("advance", "wikipedia_page"),
                    ("on-time", "imdb_title"), ("on-time", "wikipedia_page")]
    dropped = sum(op.get("numRowsDroppedByWatermark", 0)
                  for batch in progress for op in batch["stateOperators"])
    assert dropped == 2                                            # the late event, once per match type
