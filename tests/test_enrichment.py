"""Spark tests for the enrichment transformations (no network access required)."""

import json

from src import enrichment as e
from src import wiki_sources as ws

TITLES = (
    "tconst string, titleType string, primaryTitle string, startYear int, "
    "runtimeMinutes int, genres string"
)
RATINGS = "tconst string, averageRating double, numVotes long"
SELECTED = (
    "tconst string, primaryTitle string, startYear int, runtimeMinutes int, "
    "averageRating double, numVotes long, genre string, genre_rank int"
)
PEOPLE = "nconst string, primaryName string, birthYear int, deathYear int, primaryProfession string"
PLOT_ARTICLE = (
    "The Film is a 1999 movie.\n\n\n== Plot ==\nA hero   wakes up.\n\n=== Part two ===\n"
    "He wins.\n\n\n== Cast ==\nSomeone"
)
LEAD_ONLY_ARTICLE = "Another Film is a documentary.\n\n\n== Production ==\nShot in 2001."
URL_A = "https://en.wikipedia.org/wiki/A"
URL_E = "https://en.wikipedia.org/wiki/E"
URL_F = "https://en.wikipedia.org/wiki/F"
URL_P = "https://en.wikipedia.org/wiki/Jane_Doe"


def article(url, text, revision, *, missing=False, retrieved="2026-11-15T00:00:00+00:00"):
    page_url = None if missing else url
    return (url, None if missing else url.rsplit("/", 1)[1], page_url, revision, text, missing,
            retrieved, ws.WIKIPEDIA_API)


def wikidata(imdb_id, item=None, url=None, iso1=None, iso3=None):
    return (imdb_id, item, url, iso1, iso3, "2026-11-15T00:00:00+00:00", ws.WIKIDATA_SPARQL)


# --- Selection -----------------------------------------------------------------------------

def top_films(spark, **overrides):
    titles = spark.createDataFrame([
        ("tt1", "movie", "A", 2000, 90, "Drama, Comedy"),
        ("tt2", "movie", "B", 2000, 90, "Drama"),
        ("tt3", "movie", "C", 2000, 90, "Drama"),
        ("tt4", "movie", "Short", 2000, 20, "Drama"),
        ("tt5", "tvSeries", "Series", 2000, 90, "Drama"),
        ("tt6", "movie", "Too old", 1970, 90, "Drama"),
        ("tt7", "movie", "Duplicate genre", 2000, 90, "Drama,Drama"),
        ("tt8", "movie", "No genre", 2000, 90, None),
        ("tt9", "movie", "No rating", 2000, 90, "Drama"),
    ], TITLES)
    ratings = spark.createDataFrame([
        ("tt1", 7.0, 100), ("tt2", 6.0, 200), ("tt3", 8.0, 200), ("tt4", 9.0, 999),
        ("tt5", 9.0, 999), ("tt6", 9.0, 999), ("tt7", 5.0, 50), ("tt8", 9.0, 999),
    ], RATINGS)
    params = dict(first_year=1980, last_year=2025, min_minutes=40, top_n=2)
    params.update(overrides)
    rows = e.select_top_films(titles, ratings, **params).collect()
    return sorted((r.genre, r.genre_rank, r.tconst) for r in rows)


def test_top_films_rank_by_votes_then_rating_per_year_and_genre(spark):
    assert top_films(spark) == [("Comedy", 1, "tt1"), ("Drama", 1, "tt3"), ("Drama", 2, "tt2")]


def test_top_films_without_limit_keep_every_eligible_film_once_per_genre(spark):
    assert top_films(spark, top_n=None) == [
        ("Comedy", 1, "tt1"),
        ("Drama", 1, "tt3"), ("Drama", 2, "tt2"), ("Drama", 3, "tt1"), ("Drama", 4, "tt7"),
    ]


def test_top_films_respect_minimum_votes(spark):
    assert top_films(spark, top_n=None, min_votes=150) == [("Drama", 1, "tt3"), ("Drama", 2, "tt2")]


def test_principals_are_distinct_people_in_billing_order(spark):
    films = spark.createDataFrame([("tt1",), ("tt1",)], "tconst string")  # tt1 in two genres
    principals = spark.createDataFrame([
        ("tt1", 1, "nm1", "producer"), ("tt1", 4, "nm1", "actor"),
        ("tt1", 2, None, "writer"),
        ("tt1", 3, "nm2", "actress"), ("tt1", 5, "nm3", "director"), ("tt1", 6, "nm4", "writer"),
        ("tt2", 1, "nm9", "actor"),
    ], "tconst string, ordering int, nconst string, category string")
    chosen = e.select_principals(principals, films, per_film=3).collect()
    assert sorted((r.principal_rank, r.nconst, r.category) for r in chosen) == [
        (1, "nm1", "producer"), (2, "nm2", "actress"), (3, "nm3", "director"),
    ]


# --- Reading and parsing the gatherer caches -----------------------------------------------

def test_read_jsonl_enforces_schema_and_drops_torn_lines(spark, tmp_path):
    cache = tmp_path / "wikidata.jsonl"
    record = dict(zip(e.WIKIDATA_SCHEMA.fieldNames(), wikidata("tt1", "Q1", URL_A, "en")))
    record["unexpected"] = "ignored"
    cache.write_text(json.dumps(record) + '\n{"imdb_id": "tt2", "wiki', encoding="utf-8")
    frame = e.read_jsonl(spark, cache.as_posix(), e.WIKIDATA_SCHEMA)
    assert frame.columns == e.WIKIDATA_SCHEMA.fieldNames()
    assert [r.imdb_id for r in frame.collect()] == ["tt1"]


def test_wikidata_links_prefer_articles_and_collect_languages(spark):
    raw = spark.createDataFrame([
        wikidata("tt1", "Q9"),                                  # duplicate item without article
        wikidata("tt1", "Q1", URL_A, iso1="EN"),
        wikidata("tt1", "Q1", URL_A, iso1="fr", iso3="fra"),
        wikidata("tt1", "Q1", URL_A, iso3="yue"),               # no ISO 639-1 code
        wikidata("tt2"),                                        # looked up, not in Wikidata
        wikidata("bad-id", "Q5", URL_E),
    ], e.WIKIDATA_SCHEMA)
    links = {r.imdb_id: r for r in e.wikidata_links(raw).collect()}
    assert set(links) == {"tt1", "tt2"}
    assert (links["tt1"].wikidata_uri, links["tt1"].article_url) == ("Q1", URL_A)
    assert links["tt1"].original_languages == ["en", "fr", "yue"]
    assert links["tt2"].wikidata_uri is None and links["tt2"].original_languages is None
    assert links["tt1"].wikidata_checked and links["tt2"].wikidata_checked


def test_article_sections_split_and_clean_lead_and_plot(spark):
    raw = spark.createDataFrame([
        article(URL_A, "Old text.", 1, retrieved="2026-11-14T00:00:00+00:00"),
        article(URL_A, PLOT_ARTICLE, 2),
        article(URL_E, "== plot summary ==\nStory only.", 3),
        article(URL_P, "Jane Doe ( ; ) is an   actress.", 4),
        article(URL_F, None, None, missing=True),
    ], e.ARTICLE_SCHEMA)
    pages = {r.article_url: r for r in e.article_sections(raw).collect()}
    assert len(pages) == 4
    a = pages[URL_A]
    assert a.revision_id == 2                                   # newest copy kept
    assert a.lead == "The Film is a 1999 movie."
    assert a.plot == "A hero wakes up.\n\nHe wins."             # sub-heading removed
    assert a.plot_section == "Plot"
    assert pages[URL_E].lead is None and pages[URL_E].plot == "Story only."
    assert pages[URL_E].plot_section == "Plot Summary"
    assert pages[URL_P].lead == "Jane Doe is an actress." and pages[URL_P].plot is None
    assert pages[URL_F].missing and pages[URL_F].lead is None and pages[URL_F].plot_section is None


# --- Enriched outputs ------------------------------------------------------------------------

def enrichment_inputs(spark):
    links = e.wikidata_links(spark.createDataFrame([
        wikidata("tt1", "Q1", URL_A, iso1="en"),
        wikidata("tt2", "Q2"),
        wikidata("tt3"),
        wikidata("tt5", "Q5", URL_E),
        wikidata("tt6", "Q6", URL_F),
        wikidata("nm1", "Q10", URL_P),
    ], e.WIKIDATA_SCHEMA))
    articles = e.article_sections(spark.createDataFrame([
        article(URL_A, PLOT_ARTICLE, 5),
        article(URL_E, LEAD_ONLY_ARTICLE, 6),
        article(URL_F, None, None, missing=True),
        article(URL_P, "Jane Doe ( ; ) is an actress.", 7),
    ], e.ARTICLE_SCHEMA))
    return links, articles


def test_enriched_films_explain_every_outcome_and_keep_lineage(spark):
    selected = spark.createDataFrame([
        ("tt1", "A", 2000, 90, 8.0, 100, "Drama", 1),
        ("tt1", "A", 2000, 90, 8.0, 100, "Action", 2),
        *[(f"tt{i}", f"F{i}", 2000, 90, 7.0, 10, "Drama", i) for i in range(2, 7)],
    ], SELECTED)
    links, articles = enrichment_inputs(spark)
    films = e.enrich_films(selected, links, articles)
    rows = {r.tconst: r for r in films.collect()}

    assert films.count() == 6                                   # one row per film
    enriched = rows["tt1"]
    assert enriched.enrichment_status == "enriched" and enriched.enrichment_note is None
    assert enriched.synopsis == "A hero wakes up.\n\nHe wins."
    assert enriched.wikipedia_lead == "The Film is a 1999 movie."
    assert [(g.genre, g.genre_rank) for g in enriched.top_genres] == [("Action", 2), ("Drama", 1)]
    assert enriched.original_languages == ["en"]
    assert enriched.source_url == URL_A and enriched.source_revision_url == e.PERMALINK + "5"

    expected = {
        "tt2": ("Wikidata item has no English Wikipedia article", "Q2"),
        "tt3": ("No Wikidata item carries this IMDb identifier", e.SPARQL_ENDPOINT),
        "tt4": ("Not looked up yet", e.SPARQL_ENDPOINT),
        "tt5": ("Wikipedia article has no plot or synopsis section", URL_E),
        "tt6": ("English Wikipedia article could not be retrieved", URL_F),
    }
    for tconst, (note, source) in expected.items():
        row = rows[tconst]
        assert row.enrichment_status == "not_available", tconst
        assert row.enrichment_note.startswith(note), (tconst, row.enrichment_note)
        assert row.source_url == source, tconst
        assert row.synopsis is None and row.original_languages == []
    assert rows["tt5"].source_revision_url == e.PERMALINK + "6"
    assert rows["tt6"].source_revision_url is None


def test_enriched_films_flag_films_with_fewer_than_three_principals(spark):
    selected = spark.createDataFrame(
        [(f"tt{i}", f"F{i}", 2000, 90, 7.0, 10, "Drama", i) for i in (1, 2, 3)], SELECTED
    )
    chosen = spark.createDataFrame(
        [("tt1", f"nm{i}", i, i, "actor") for i in (1, 2, 3)] + [("tt2", "nm9", 1, 1, "actor")],
        "tconst string, nconst string, ordering int, principal_rank int, category string",
    )
    links, articles = enrichment_inputs(spark)
    rows = {r.tconst: r for r in e.enrich_films(selected, links, articles, chosen).collect()}
    assert (rows["tt1"].principal_count, rows["tt1"].principals_note) == (3, None)
    assert rows["tt2"].principal_count == 1 and rows["tt2"].principals_note.startswith("IMDb lists only 1")
    assert rows["tt3"].principal_count == 0 and "fewer than 3" in rows["tt3"].principals_note
    unchecked = e.enrich_films(selected, links, articles).first()
    assert unchecked.principal_count is None and unchecked.principals_note is None


def test_enriched_principals_carry_biography_and_notes(spark):
    chosen = spark.createDataFrame(
        [("tt1", "nm1", 1, 1, "actress"), ("tt1", "nm2", 2, 2, "actor")],
        "tconst string, nconst string, ordering int, principal_rank int, category string",
    )
    people = spark.createDataFrame([("nm1", "Jane Doe", 1970, None, "actress,producer")], PEOPLE)
    links, articles = enrichment_inputs(spark)
    rows = {r.nconst: r for r in e.enrich_principals(chosen, people, links, articles).collect()}

    jane = rows["nm1"]
    assert (jane.primaryName, jane.birthYear, jane.category) == ("Jane Doe", 1970, "actress")
    assert jane.biography == "Jane Doe is an actress." and jane.enrichment_status == "enriched"
    assert jane.source_url == URL_P and jane.source_revision_url == e.PERMALINK + "7"

    unknown = rows["nm2"]
    assert unknown.primaryName is None and unknown.biography is None
    assert unknown.enrichment_note.startswith("Not looked up yet")
    assert unknown.source_url == e.SPARQL_ENDPOINT


def test_coverage_counts_and_percentages(spark):
    frame = spark.createDataFrame(
        [("enriched", None)] * 3 + [("not_available", "missing")],
        "enrichment_status string, enrichment_note string",
    )
    rows = [(r.enrichment_status, r["count"], r.pct) for r in e.coverage(frame).collect()]
    assert rows == [("enriched", 3, 75.0), ("not_available", 1, 25.0)]
