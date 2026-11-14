"""Unit tests for the streaming producer and pipeline logic.

These tests do not require Kafka or Docker: they only exercise pure
transformation functions on local Spark DataFrames.
"""

import datetime as dt

from src.streaming_pipeline import parse_events, with_event_time
from src.streaming_producer import is_relevant


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