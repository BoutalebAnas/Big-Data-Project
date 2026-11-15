import pytest

from src import exploration as q

FILM_SCHEMA = (
    "tconst string, primaryTitle string, startYear int, runtimeMinutes int, "
    "genres string, averageRating double, numVotes long"
)


def people(spark, *ids):
    return spark.createDataFrame([(key, key.upper()) for key in ids], "nconst string, primaryName string")


def test_outliers_use_robust_genre_scores_and_exclude_shorts(spark):
    runtimes = [90, 95, 100, 105, 110, 300]
    films = spark.createDataFrame(
        [(f"tt{i}", str(i), 2000, minutes, "Drama,Drama", None, None) for i, minutes in enumerate(runtimes)]
        + [("tt9", "Short", 2000, 20, "Drama", None, None)],
        FILM_SCHEMA,
    )
    outliers = q.runtime_outliers(films, min_minutes=40, min_genre_films=3, min_robust_z=3.5).collect()
    assert [row.tconst for row in outliers] == ["tt5"]          # the short film is not a feature
    row = outliers[0]
    assert (row.genre_films, row.genre_median_minutes, row.genre_mad_minutes) == (6, 100, 5)
    assert row.robust_z == pytest.approx(0.6745 * 200 / 5)
    assert row.genre_mean_minutes == pytest.approx(sum(runtimes) / 6)
    # The classic z-score is diluted by the extreme value itself and would miss it at 3.5.
    assert row.z_score < 3.5


def test_genre_rank_and_comparison_reconcile_votes(spark):
    films = spark.createDataFrame([
        ("tt1", "A", 1980, 60, "Drama,Comedy", 8.0, 100),
        ("tt2", "B", 1980, 60, "Drama,Drama", 6.0, 100),
        ("tt3", "C", 1980, 60, "Horror", None, None),
        ("tt4", "D", 1979, 60, "Action", 9.0, 900),
    ], FILM_SCHEMA)
    ranked = q.genre_rankings(films, first_year=1980, last_year=2025)
    assert [(row.genre, row.votes, row.films) for row in ranked.orderBy("rank").collect()] == [
        ("Drama", 200, 2), ("Comedy", 100, 1), ("Horror", 0, 1),
    ]
    groups = {row.group: row for row in q.compare_genres(ranked, top_n=1).collect()}
    assert groups["top"].vote_share == pytest.approx(2 / 3)
    assert groups["top"].weighted_rating == 7
    assert groups["rest"].rated_film_genre_pairs == 1


def test_directors_deduplicate_credits_and_exclude_second_hit(spark):
    films = spark.createDataFrame([
        ("tt1", 8.0), ("tt2", 5.0), ("tt3", 7.0), ("tt4", 9.0), ("tt5", None),
    ], "tconst string, averageRating double")
    crew = spark.createDataFrame([
        ("tt1", "nm1,nm1,nm2,nm3"), ("tt2", "nm1,nm2,nm3"), ("tt3", "nm1,nm2,nm3"),
        ("tt4", "nm2"), ("tt5", "nm1"),
    ], "tconst string, directors string")
    result = q.directors_performance(
        films, crew, people(spark, "nm1", "nm2"), high_rating=8.0,
        max_other_mean=6.0, min_other_films=2,
    ).collect()
    assert [(r.nconst, r.other_mean, r.unrated_films) for r in result] == [("nm1", 6.0, 1)]


def test_series_uses_last_observed_season_and_rejects_worst_tie(spark):
    titles = [("tt1", "One", "tvSeries"), ("tt2", "Two", "tvSeries")]
    episodes, ratings = [], []
    for series in (1, 2):
        for season, scores in enumerate(((9.0, 7.0), (8.0, 6.0), (5.0, 4.0), (3.0,)), 1):
            for index, score in enumerate(scores, 1):
                episode = f"tt{series}{season}{index}"
                titles.append((episode, episode, "tvEpisode"))
                episodes.append((episode, f"tt{series}", season))
                ratings.append((episode, 3.0 if series == 2 and season == 3 and index == 2 else score, 100))
    result = q.series_evolution(
        spark.createDataFrame(titles, "tconst string, primaryTitle string, titleType string"),
        spark.createDataFrame(episodes, "tconst string, parentTconst string, seasonNumber int"),
        spark.createDataFrame(ratings, "tconst string, averageRating double, numVotes long"),
        min_seasons=3, min_episodes=2, min_votes=1,
    ).collect()
    assert [(r.parentTconst, r.qualifying_seasons, r.last_season) for r in result] == [("tt1", 3, 4)]


def test_languages_keep_provenance_and_require_more_than_one_film(spark, tmp_path):
    reference = tmp_path / "original_languages.csv"
    reference.write_text(
        "tconst,language,source_uri\n"
        "tt1, FR ,https://example.org/a\n"
        "tt1,en,https://example.org/a\n"
        "tt1,en,https://example.org/b\n"
        "tt2,de,https://example.org/c\n",
        encoding="utf-8",
    )
    languages = q.verified_film_languages(spark, reference)
    successful = spark.createDataFrame([("tt1",), ("tt2",)], "tconst string")
    actors = q.acting_credits(spark.createDataFrame([
        ("tt1", "nm1", "actress"), ("tt1", "nm1", "actor"), ("tt2", "nm1", "actress"),
        ("tt1", "nm2", "actor"), ("tt2", "nm2", "writer"),
    ], "tconst string, nconst string, category string"))
    result = q.multilingual_actors(successful, actors, people(spark, "nm1", "nm2"), languages).collect()
    assert len(result) == 1 and result[0].nconst == "nm1" and result[0].language_count == 3
    assert [(x.language, x.films, x.example.source_uri) for x in result[0].languages] == [
        ("de", 1, "https://example.org/c"), ("en", 1, "https://example.org/a"),
        ("fr", 1, "https://example.org/a"),
    ]


def test_original_title_language_excludes_localized_and_conflicting_codes(spark):
    akas = spark.createDataFrame([
        ("tt1", 1, "fr"), ("tt1", 0, "en"), ("tt1", 1, "fr"),
        ("tt2", 1, "de"), ("tt2", 1, "en"), ("tt3", 1, None), ("tt4", 1, "und"),
    ], "titleId string, isOriginalTitle int, language string")
    assert [(r.tconst, r.language) for r in q.original_title_languages(akas).collect()] == [("tt1", "fr")]


def test_career_peak_requires_later_rating_and_excludes_future_films(spark):
    films = spark.createDataFrame([
        ("tt1", 2000, 8.0, 100), ("tt2", 2005, 7.0, 100), ("tt3", 2020, 6.0, 100),
        ("tt4", 2030, 9.0, 100), ("tt5", 2020, 8.0, 100),
    ], "tconst string, startYear int, averageRating double, numVotes long")
    credits = spark.createDataFrame([
        ("tt1", "nm1"), ("tt2", "nm1"), ("tt3", "nm1"), ("tt4", "nm1"),
        ("tt1", "nm2"), ("tt2", "nm2"), ("tt5", "nm2"),
    ], "tconst string, nconst string")
    peaks = q.early_career_peaks(
        films, credits, people(spark, "nm1", "nm2"), through_year=2026,
        min_years=20, min_films=3, early_years=5, min_votes=100,
    ).collect()
    assert [(r.nconst, r.career_years, r.early_peak.tconst, r.later_peak_rating) for r in peaks] == [
        ("nm1", 20, "tt1", 7.0),
    ]


def test_multi_role_uses_one_rating_per_film_and_normalizes_acting(spark):
    films = spark.createDataFrame([
        ("tt1", 8.0, 100), ("tt2", 6.0, 100), ("tt3", 2.0, 5),
    ], "tconst string, averageRating double, numVotes long")
    crew = spark.createDataFrame([("tt1", "nm1,nm1", None)], "tconst string, directors string, writers string")
    principals = spark.createDataFrame([
        ("tt1", "nm1", "actress"), ("tt1", "nm1", "director"),
        ("tt2", "nm1", "actor"), ("tt2", "nm1", "actress"),
        ("tt3", "nm1", "actor"), ("tt3", "nm1", "self"),
    ], "tconst string, nconst string, category string")
    result = q.multi_role_performance(films, crew, principals, people(spark, "nm1"), min_votes=100).first()
    assert result.career_roles == ["actor", "director"]
    assert (result.multi_role_films, result.single_role_films, result.single_rated_films) == (1, 2, 1)
    assert result.rating_delta == 2.0


def test_multi_role_summary_compares_people_with_themselves_per_combination(spark):
    films = spark.createDataFrame([
        ("tt1", 8.0, 100), ("tt2", 6.0, 100), ("tt3", 7.0, 100), ("tt4", 9.0, 100),
        ("tt5", 5.0, 100), ("tt6", 4.0, 100), ("tt7", 9.9, 5),
    ], "tconst string, averageRating double, numVotes long")
    crew = spark.createDataFrame([
        ("tt1", "nm1", "nm1"), ("tt3", "nm2", None), ("tt5", "nm3", "nm3"), ("tt7", "nm1", None),
    ], "tconst string, directors string, writers string")
    principals = spark.createDataFrame([
        ("tt1", "nm1", "actor"),                                  # nm1: actor+director+writer on tt1
        ("tt2", "nm1", "actor"),                                  # nm1: single role, 6.0
        ("tt3", "nm2", "actress"), ("tt3", "nm2", "producer"),    # nm2: actor+director+producer on tt3
        ("tt4", "nm2", "actor"),                                  # nm2: single role, 9.0
        ("tt6", "nm3", "actor"),                                  # nm3: single role, 4.0
        ("tt7", "nm1", "actor"),                                  # too few votes: ignored in means
    ], "tconst string, nconst string, category string")
    rows = {r.combination: r for r in q.multi_role_summary(films, crew, principals, min_votes=100).collect()}
    assert set(rows) == {"any", "actor+director+writer", "actor+director", "director+writer"}
    assert rows["any"].people == 3
    assert rows["any"].mean_rating_delta == pytest.approx((2.0 - 2.0 + 1.0) / 3)
    assert rows["any"].share_better_multi == pytest.approx(2 / 3)
    assert rows["actor+director+writer"].people == 1 and rows["actor+director+writer"].mean_rating_delta == 2.0
    assert rows["actor+director"].mean_rating_delta == -2.0            # nm2, producer ignored
    assert rows["director+writer"].mean_rating_delta == 1.0            # nm3
