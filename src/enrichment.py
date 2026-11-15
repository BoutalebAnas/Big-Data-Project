
from pyspark.sql import Window, functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

# there we set the schema for the dataframes containing wikidata and wikipedia data

#the dataframe with this schema will contain data from wikidata 
WIKIDATA_SCHEMA = StructType([
    StructField("imdb_id", StringType()),
    StructField("wikidata_uri", StringType()),
    StructField("article_url", StringType()),
    StructField("language_iso1", StringType()),
    StructField("language_iso3", StringType()),
    StructField("retrieved_at", StringType()),
    StructField("source", StringType()),
])
# the dataframe who will have this schema will get data from the wikipedia article
ARTICLE_SCHEMA = StructType([
    StructField("article_url", StringType()),
    StructField("page_title", StringType()),
    StructField("page_url", StringType()),
    StructField("revision_id", LongType()),
    StructField("text", StringType()),
    StructField("missing", BooleanType()),
    StructField("retrieved_at", StringType()),
    StructField("source", StringType()),
])

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
PERMALINK = "https://en.wikipedia.org/w/index.php?oldid="
# Level-2 heading of the plot section, then everything up to the next level-2
# heading (level-3 sub-headings such as "=== Part one ===" stay inside).
PLOT_SECTION = (
    r"(?si)\n==\s*(plot|plot summary|synopsis|premise|summary|story|storyline)\s*==[ \t]*\n"
    r"(.*?)(?=\n==[^=]|\z)"
)
LEAD_SECTION = r"(?s)^(.*?)(?=\n==[^=]|\z)"

#the idea of this function is to select the top 10 films each yaear.
#For popularity we use the IMDB votes
# to avoid getting short films in the final top we set a minimum of lengh for each film to qualify
# we flter out the film with no votes. No need to compare them to known which is more popular
#We only enrich movies from 1980 TO 2025


def select_top_films(titles, ratings, *, first_year, last_year, min_minutes, top_n,min_votes=1):
    #we create a datframe containing the films and the ratings of the film from the dataframes 
    #titles and ratings
    films = (
        titles.filter(
            (F.col("titleType") == "movie") # only movies
            & (F.col("runtimeMinutes") >= min_minutes)
            & F.col("startYear").between(first_year, last_year)
        )
        .join(ratings.filter(F.col("numVotes") >= min_votes), "tconst")
    )
    #we create the dataframe genre from the films one where we keep only what we need
    #we ignore the films without a genre
    genres = films.select(
        "tconst", "primaryTitle", "startYear", "runtimeMinutes", "averageRating", "numVotes",
        F.explode(F.array_distinct(F.transform(F.split("genres", ","), F.trim))).alias("genre"),
    ).filter(F.col("genre") != "") 
    

    #windowspec : we will use it to group the dataframe by year and genre to filter the top ten of each
    order = Window.partitionBy("startYear", "genre").orderBy(
        F.desc("numVotes"), F.desc("averageRating"), "tconst"
    )
    # we create a column in the dataframe to store the film ranks among each year and genre 
    ranked = genres.withColumn("genre_rank", F.row_number().over(order))
    #
    return ranked if top_n is None else ranked.filter(F.col("genre_rank") <= top_n) #the filter 


def select_principals(principals, films, *, per_film):
    #we create the credit dataframe
    # By joining principals and the films dataframe we get the principals from each film
    credits = (
        # a film can appear several time in the selected film dataframe if it has two genre
        # we use distinct to avoid that
        principals.join(films.select("tconst").distinct(), "tconst", "left_semi")#
        .filter(F.col("nconst").isNotNull())# if somehow a film doesn't have principals or if they have no id
        .groupBy("tconst", "nconst")
        .agg(
            F.min("ordering").alias("ordering"),
            # ordering is in principal to order their importance
            F.min_by("category", "ordering").alias("category"),
        )# avoid getting someone who is both actor and director for exemple
    )
    order = Window.partitionBy("tconst").orderBy("ordering", "nconst")# Windowspec to group 
    #the dataframe by movie title
    return (
        credits.withColumn("principal_rank", F.row_number().over(order))
        .filter(F.col("principal_rank") <= per_film)# only keep the selected number of top principals
    )


def write_ids(frame, column, path):
    """Hand identifiers to the (non-Spark) gatherer as CSV part files."""
    frame.select(column).distinct().write.mode("overwrite").option("header", True).csv(path)


def read_jsonl(spark, path, schema):
    """Read a gatherer cache with an explicit schema; malformed lines are dropped."""
    return (
        spark.read.schema(schema).option("mode", "DROPMALFORMED").json(path)
    )


def wikidata_links(raw):
    """One Wikidata item and article per IMDb id, plus the film's original languages."""
    raw = raw.filter(F.col("imdb_id").rlike(r"^(tt|nm)[0-9]+$"))
    prefer = Window.partitionBy("imdb_id").orderBy(
        F.col("article_url").asc_nulls_last(), F.col("wikidata_uri").asc_nulls_last()
    )
    links = (
        raw.withColumn("pick", F.row_number().over(prefer)).filter(F.col("pick") == 1)
        .select("imdb_id", "wikidata_uri", "article_url",
                F.col("retrieved_at").alias("wikidata_retrieved_at"),
                F.lit(True).alias("wikidata_checked"))
    )
    languages = (
        raw.select("imdb_id", F.lower(F.coalesce("language_iso1", "language_iso3")).alias("language"))
        .filter(F.col("language").isNotNull())
        .groupBy("imdb_id")
        .agg(F.array_sort(F.collect_set("language")).alias("original_languages"))
    )
    return links.join(languages, "imdb_id", "left")

# functions to clean text. I won't go over each one  but thay allow primarly cleaning of char 
#you get 
def _clean_text(column):
    text = F.regexp_replace(column, r"(?m)^=+[^=\n]+=+[ \t]*$", "")  # sub-headings
    text = F.regexp_replace(text, r"\(\s*[;,]?\s*\)", "")             # emptied pronunciations
    text = F.regexp_replace(text, r"[ \t]+", " ")
    text = F.regexp_replace(text, r"\s*\n\s*\n\s*", "\n\n")
    text = F.trim(text)
    return F.when(F.length(text) > 0, text)

# the gatherer is suppose to get only one article per movie but here we filter the one with missing url
# and doubles
def article_sections(raw):
    """Latest copy of each article, split into lead and plot section."""
    latest = Window.partitionBy("article_url").orderBy(F.desc("retrieved_at"))
    pages = (
        raw.filter(F.col("article_url").isNotNull())
        .withColumn("pick", F.row_number().over(latest)).filter(F.col("pick") == 1)
    )
    text = F.concat(F.lit("\n"), F.coalesce(F.col("text"), F.lit("")))
    return pages.select(
        "article_url", "page_title", "page_url", "revision_id", "missing",
        F.col("retrieved_at").alias("article_retrieved_at"),
        _clean_text(F.regexp_extract(text, LEAD_SECTION, 1)).alias("lead"),
        _clean_text(F.regexp_extract(text, PLOT_SECTION, 2)).alias("plot"),
        F.when(F.regexp_extract(text, PLOT_SECTION, 1) != "",
               F.initcap(F.regexp_extract(text, PLOT_SECTION, 1))).alias("plot_section"),
    )


def _lineage():
# this function allows us to know what failed or succeeded. If we can't fnd the imdb id in 
# wikidata or if wikidata doesn't have any url to link an article
   
    reason = (
        F.when(F.col("wikidata_checked").isNull(),
               F.lit("Not looked up yet: gathering was limited or interrupted"))
        .when(F.col("wikidata_uri").isNull(),
              F.lit(f"No Wikidata item carries this IMDb identifier (P345); checked via {SPARQL_ENDPOINT}"))
        .when(F.col("article_url").isNull(),
              F.lit("Wikidata item has no English Wikipedia article"))
        .when(F.col("revision_id").isNull() | F.coalesce(F.col("missing"), F.lit(False)),
              F.lit("English Wikipedia article could not be retrieved"))
    )
    source = F.coalesce(F.col("page_url"), F.col("article_url"), F.col("wikidata_uri"), F.lit(SPARQL_ENDPOINT))
    revision = F.when(F.col("revision_id").isNotNull(),
                      F.concat(F.lit(PERMALINK), F.col("revision_id").cast("string")))
    return reason, source, revision


def enrich_films(selected, links, articles):
    # the function to create enrich dataframes 
    memberships = selected.groupBy("tconst").agg( 
        F.first("primaryTitle").alias("primaryTitle"),
        F.first("startYear").alias("startYear"),
        F.first("runtimeMinutes").alias("runtimeMinutes"),
        F.first("averageRating").alias("averageRating"),
        F.first("numVotes").alias("numVotes"),
        F.array_sort(F.collect_list(F.struct("genre", "genre_rank"))).alias("top_genres"),
    )# select what we need from the films from imdb
    joined = (
        memberships.join(links, memberships.tconst == links.imdb_id, "left").drop("imdb_id")
        .join(articles, "article_url", "left")
    )# join the articles url
    reason, source, revision = _lineage()
    note = F.coalesce(
        reason,
        F.when(F.col("plot").isNull(), F.lit("Wikipedia article has no plot or synopsis section")),
    )# add a plot column and fill with message if empty
    return joined.select(
        F.col("tconst").cast("string"),
        F.col("primaryTitle").cast("string"),
        F.col("startYear").cast("int"),
        F.col("runtimeMinutes").cast("int"),
        F.col("averageRating").cast("double"),
        F.col("numVotes").cast("long"),
        "top_genres",
        F.coalesce("original_languages", F.array().cast("array<string>")).alias("original_languages"),
        F.col("plot").alias("synopsis"),
        "plot_section",
        F.col("lead").alias("wikipedia_lead"),
        F.when(note.isNull(), F.lit("enriched")).otherwise(F.lit("not_available")).alias("enrichment_status"),
        note.alias("enrichment_note"),
        source.alias("source_url"),
        revision.alias("source_revision_url"),
        "wikidata_uri",
        F.coalesce("article_retrieved_at", "wikidata_retrieved_at").alias("retrieved_at"),
    )


def enrich_principals(chosen, people, links, articles):
    # function to enrich principals
    joined = (
        chosen.join(people.select("nconst", "primaryName", "birthYear", "deathYear", "primaryProfession"),
                    "nconst", "left")
        .join(links, F.col("nconst") == links.imdb_id, "left").drop("imdb_id")
        .join(articles, "article_url", "left")
    )# join the people table and the chosen one to get all people info
    #join links to get the the wikidata
    #join the article from the article dataframe
    reason, source, revision = _lineage()
    note = F.coalesce(
        reason,
        F.when(F.col("lead").isNull(), F.lit("Wikipedia article has no introduction text")),
    )# add the introduction from wikipedia and write message if not existant
    return joined.select(
        F.col("tconst").cast("string"),
        F.col("nconst").cast("string"),
        F.col("principal_rank").cast("int"),
        F.col("category").cast("string"),
        F.col("primaryName").cast("string"),
        F.col("birthYear").cast("int"),
        F.col("deathYear").cast("int"),
        F.col("primaryProfession").cast("string"),
        F.col("lead").alias("biography"),
        F.when(note.isNull(), F.lit("enriched")).otherwise(F.lit("not_available")).alias("enrichment_status"),
        note.alias("enrichment_note"),
        source.alias("source_url"),
        revision.alias("source_revision_url"),
        "wikidata_uri",
        F.coalesce("article_retrieved_at", "wikidata_retrieved_at").alias("retrieved_at"),
    )


def coverage(frame):
    """Share of records enriched, with the breakdown of notes."""
    return (
        frame.groupBy("enrichment_status", "enrichment_note").count()
        .withColumn("pct", F.round(100 * F.col("count") / F.sum("count").over(Window.partitionBy()), 2))
        .orderBy(F.desc("count"))
    )
