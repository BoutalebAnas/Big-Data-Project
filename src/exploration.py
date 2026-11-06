"""Distributed queries for the eight IMDb exploration questions."""

from pyspark.sql import Window, functions as F


def movies(titles, ratings):
    return titles.filter(F.col("titleType") == "movie").select(
        "tconst", "primaryTitle", "startYear", "runtimeMinutes", "genres"
    ).join(ratings, "tconst", "left")


def _movie_genres(films):
    return films.select(
        "*", F.explode(F.array_distinct(F.transform(F.split("genres", ","), F.trim))).alias("genre")
    ).filter(F.col("genre") != "").drop("genres")


def runtime_outliers(films, *, min_minutes, min_genre_films, min_z):
    features = _movie_genres(films.filter(F.col("runtimeMinutes") >= min_minutes))
    genre_window = Window.partitionBy("genre")
    return (
        features.select("tconst", "primaryTitle", "startYear", "genre", "runtimeMinutes")
        .withColumn("genre_films", F.count("*").over(genre_window))
        .withColumn("genre_mean_minutes", F.avg("runtimeMinutes").over(genre_window))
        .withColumn("genre_sd_minutes", F.stddev_pop("runtimeMinutes").over(genre_window))
        .filter((F.col("genre_films") >= min_genre_films) & (F.col("genre_sd_minutes") > 0))
        .withColumn("delta_minutes", F.col("runtimeMinutes") - F.col("genre_mean_minutes"))
        .withColumn("z_score", F.col("delta_minutes") / F.col("genre_sd_minutes"))
        .filter(F.abs("z_score") >= min_z)
    )


def genre_rankings(films, *, first_year, last_year):
    annual = _movie_genres(films.filter(F.col("startYear").between(first_year, last_year)))
    rated = F.col("averageRating").isNotNull() & (F.col("numVotes") > 0)
    metrics = annual.groupBy(F.col("startYear").alias("year"), "genre").agg(
        F.count("*").alias("films"),
        F.count_if(rated).alias("rated_films"),
        F.sum(F.when(rated, F.col("numVotes")).otherwise(0)).alias("votes"),
        F.sum(F.when(rated, F.col("averageRating") * F.col("numVotes")).otherwise(0.0))
        .alias("rating_vote_points"),
    )
    rank = Window.partitionBy("year").orderBy(F.desc("votes"), "genre")
    return metrics.withColumn("rank", F.row_number().over(rank))


def compare_genres(ranked, *, top_n):
    year = Window.partitionBy("year")
    return (
        ranked.withColumn("group", F.when(F.col("rank") <= top_n, "top").otherwise("rest"))
        .groupBy("year", "group").agg(
            F.count("*").alias("genres"),
            F.sum("films").alias("film_genre_pairs"),
            F.sum("rated_films").alias("rated_film_genre_pairs"),
            F.sum("votes").alias("votes"),
            F.sum("rating_vote_points").alias("rating_vote_points"),
        )
        .withColumn("vote_share", F.try_divide("votes", F.sum("votes").over(year)))
        .withColumn("votes_per_film_genre", F.try_divide("votes", "film_genre_pairs"))
        .withColumn("weighted_rating", F.try_divide("rating_vote_points", "votes"))
        .drop("rating_vote_points")
    )


def _crew_credits(crew, column, role):
    return (
        crew.select("tconst", F.explode(F.split(F.col(column), ",")).alias("nconst"))
        .withColumn("nconst", F.trim("nconst")).filter(F.col("nconst") != "")
        .withColumn("role", F.lit(role))
    )


def _with_names(frame, people):
    return frame.join(people.select("nconst", "primaryName"), "nconst")


def directors_performance(films, crew, people, *, high_rating, max_other_mean, min_other_films):
    directors = _crew_credits(crew, "directors", "director").select("tconst", "nconst").distinct()
    rated = directors.join(films.select("tconst", "averageRating"), "tconst")
    high = F.col("averageRating") >= high_rating
    other = F.col("averageRating") < high_rating
    statistics = rated.groupBy("nconst").agg(
        F.count_if(high).alias("high_films"),
        F.max(F.when(high, F.struct("tconst", "averageRating"))).alias("high_film"),
        F.count_if(other).alias("other_rated_films"),
        F.avg(F.when(other, F.col("averageRating"))).alias("other_mean"),
        F.count_if(F.col("averageRating").isNull()).alias("unrated_films"),
    ).filter(
        (F.col("high_films") == 1) & (F.col("other_rated_films") >= min_other_films)
        & (F.col("other_mean") <= max_other_mean)
    )
    return _with_names(statistics, people)


def series_evolution(titles, episodes, ratings, *, min_seasons, min_episodes, min_votes):
    episode_titles = titles.filter(F.col("titleType") == "tvEpisode").select("tconst")
    series_titles = titles.filter(F.col("titleType").isin("tvSeries", "tvMiniSeries")).select(
        F.col("tconst").alias("parentTconst"), F.col("primaryTitle").alias("series_title")
    )
    numbered = episodes.filter(F.col("seasonNumber") >= 1).join(episode_titles, "tconst", "left_semi")
    rated = numbered.join(
        ratings.filter(F.col("numVotes") >= min_votes).select("tconst", "averageRating"),
        "tconst", "left",
    )
    score = F.when(F.col("averageRating").isNotNull(), F.struct("averageRating", "seasonNumber", "tconst"))
    by_season = rated.groupBy("parentTconst", "seasonNumber").agg(
        F.count("*").alias("episodes"),
        F.count("averageRating").alias("rated_episodes"),
        F.min(score).alias("worst"),
        F.max(score).alias("best"),
    )
    return (
        by_season.groupBy("parentTconst").agg(
            F.count_if(F.col("episodes") >= min_episodes).alias("qualifying_seasons"),
            F.count("*").alias("observed_seasons"),
            F.max("seasonNumber").alias("last_season"),
            F.sum("episodes").alias("episodes"),
            F.sum("rated_episodes").alias("rated_episodes"),
            F.min("worst").alias("worst_episode"),
            F.max("best").alias("best_episode"),
        )
        .filter((F.col("qualifying_seasons") >= min_seasons)
                & (F.col("worst_episode.seasonNumber") == F.col("last_season"))
                & (F.col("best_episode.seasonNumber") <= 2))
        .join(series_titles, "parentTconst")
    )


def acting_credits(principals):
    return (
        principals.filter(F.col("category").isin("actor", "actress") & F.col("nconst").isNotNull())
        .select("tconst", "nconst").distinct()
    )


def _language_code():
    return F.lower(F.trim("language"))


def _known_language_code():
    language = F.col("language")
    return language.rlike(r"^[a-z]{2,3}$") & ~language.isin("mis", "mul", "und", "zxx")


def original_title_languages(akas):
    """Infer original *title* language only when explicit codes agree."""
    return (
        akas.filter(F.col("isOriginalTitle") == 1).select(
            F.col("titleId").alias("tconst"), _language_code().alias("language")
        )
        .filter(_known_language_code())
        .groupBy("tconst").agg(F.min("language").alias("language"), F.max("language").alias("_last"))
        .filter(F.col("language") == F.col("_last"))
        .select("tconst", "language", F.lit(None).cast("string").alias("source_uri"))
    )


def verified_film_languages(spark, path):
    """Load curated original spoken language claims with their source URLs."""
    reference = spark.read.schema("tconst string, language string, source_uri string").options(
        header="true", mode="FAILFAST", enforceSchema="false"
    ).csv(path.as_posix()).select(
        F.trim("tconst").alias("tconst"), _language_code().alias("language"),
        F.trim("source_uri").alias("source_uri"),
    )
    valid = (
        F.col("tconst").rlike(r"^tt[0-9]+$") & _known_language_code()
        & F.col("source_uri").rlike(r"^https?://[^\s/]+[^\s]*$")
    )
    if reference.filter(~F.coalesce(valid, F.lit(False))).take(1):
        raise ValueError("Le CSV de Q6 exige tconst, un code de langue normalisé et une URL par ligne.")
    if not reference.take(1):
        raise ValueError("Le CSV de Q6 est vide.")
    return reference.groupBy("tconst", "language").agg(F.min("source_uri").alias("source_uri"))


def multilingual_actors(successful_films, actors, people, languages, *, min_languages=2):
    matches = successful_films.select("tconst").join(languages, "tconst").join(actors, "tconst")
    by_language = matches.groupBy("nconst", "language").agg(
        F.count("*").alias("films"),
        F.min(F.struct("tconst", "source_uri")).alias("example"),
        F.max("tconst").alias("_last_film"),
    )
    by_actor = by_language.groupBy("nconst").agg(
        F.count("*").alias("language_count"),
        F.min("example.tconst").alias("_first_film"),
        F.max("_last_film").alias("_last_film"),
        F.sort_array(F.collect_list(F.struct("language", "films", "example"))).alias("languages"),
    )
    across_films = by_actor.filter(
        (F.col("language_count") >= min_languages) & (F.col("_first_film") != F.col("_last_film"))
    ).drop("_first_film", "_last_film")
    return _with_names(across_films, people)


def early_career_peaks(films, actors, people, *, through_year, min_years, min_films, early_years, min_votes):
    dated = films.filter(F.col("startYear") <= through_year).join(actors, "tconst")
    career = Window.partitionBy("nconst")
    dated = dated.withColumn("first_year", F.min("startYear").over(career))
    early = F.col("startYear") < F.col("first_year") + early_years
    rating = F.when(F.col("numVotes") >= min_votes, F.col("averageRating"))
    return _with_names(
        dated.groupBy("nconst").agg(
            F.min("first_year").alias("first_year"),
            F.max("startYear").alias("last_year"),
            F.count("*").alias("dated_films"),
            F.count_if(early & rating.isNotNull()).alias("early_rated_films"),
            F.count_if(~early & rating.isNotNull()).alias("later_rated_films"),
            F.max(F.when(early & rating.isNotNull(),
                         F.struct("averageRating", "numVotes", "tconst", "startYear"))).alias("early_peak"),
            F.max(F.when(~early, rating)).alias("later_peak_rating"),
        ).withColumn("career_years", F.col("last_year") - F.col("first_year"))
        .filter((F.col("career_years") >= min_years) & (F.col("dated_films") >= min_films)
                & (F.col("early_peak.averageRating") > F.col("later_peak_rating"))),
        people,
    )


def multi_role_performance(films, crew, principals, people, *, min_votes):
    credits = principals.select(
        "tconst", "nconst", F.lower(F.trim("category")).alias("role")
    ).filter(
        F.col("nconst").isNotNull() & (F.col("role") != "")
        & ~F.col("role").isin("self", "archive_footage", "archive_sound")
    ).withColumn("role", F.when(F.col("role") == "actress", "actor").otherwise(F.col("role")))
    all_credits = credits.unionByName(_crew_credits(crew, "directors", "director")).unionByName(
        _crew_credits(crew, "writers", "writer")
    )
    projects = all_credits.join(films.select("tconst", "averageRating", "numVotes"), "tconst").groupBy(
        "nconst", "tconst", "averageRating", "numVotes"
    ).agg(F.sort_array(F.collect_set("role")).alias("roles"))
    multi = F.size("roles") >= 2
    rating = F.when(F.col("numVotes") >= min_votes, F.col("averageRating"))
    profiles = projects.groupBy("nconst").agg(
        F.sort_array(F.array_distinct(F.flatten(F.collect_set("roles")))).alias("career_roles"),
        F.count_if(multi).alias("multi_role_films"),
        F.count_if(~multi).alias("single_role_films"),
        F.count_if(multi & rating.isNotNull()).alias("multi_rated_films"),
        F.count_if(~multi & rating.isNotNull()).alias("single_rated_films"),
        F.avg(F.when(multi, rating)).alias("multi_mean_rating"),
        F.avg(F.when(~multi, rating)).alias("single_mean_rating"),
        F.min(F.when(multi, F.struct("tconst", "roles"))).alias("multi_example"),
    ).filter(F.size("career_roles") >= 2).withColumn(
        "rating_delta", F.col("multi_mean_rating") - F.col("single_mean_rating")
    )
    return _with_names(profiles, people)
