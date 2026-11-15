# IMDb Project Report

This report compiles the final outputs of the Spark processing and, for each section, the required methodological explanation. The tables are query outputs. The explanations, choices, and conclusions are written by the group members.

## Contributions

| Part | Member (GitHub) | Pull Requests |
| --- | --- | --- |
| Data Preparation | thomassbgn | #2 |
| Data Exploration | thomassbgn | #2, #4, fixes for Q1, Q6 and Q8 |
| Data Enrichment | TaDLpZ | #5, cleaning fixes |
| Streaming | BoutalebAnas | #3, IMDb matching fix and late data handling |
| Open Question | BoutalebAnas | #6 |
| Repository Hygiene | All | `.gitignore` for enrichment outputs |

## Data Sources

- IMDb snapshot: seven `*.tsv.gz` archives from the IMDb distribution point, preparation `452e5d1ccb1142b3823d9ba935e401a0` (November 6, 2026, validation year 2026).
- Enrichment: Wikidata (P345 IMDb ID, P364 original language) and English Wikipedia, via their public APIs.
- Streaming: Wikimedia EventStreams `recentchange` feed, transported by Kafka.

---

## 1. Data Preparation

### Missing Values per Column

Nulls, empty strings, whitespace strings, and the IMDb `\N` marker, measured on all raw rows.

| table | column | rows | missing | missing_pct |
| --- | --- | --- | --- | --- |
| name.basics | birthYear | 15,793,855 | 15,108,483 | 95.661 |
| name.basics | deathYear | 15,793,855 | 15,528,603 | 98.321 |
| name.basics | knownForTitles | 15,793,855 | 1,914,559 | 12.122 |
| name.basics | nconst | 15,793,855 | 0 | 0 |
| name.basics | primaryName | 15,793,855 | 89 | 0.001 |
| name.basics | primaryProfession | 15,793,855 | 3,213,175 | 20.344 |
| title.akas | attributes | 60,231,176 | 59,914,206 | 99.474 |
| title.akas | isOriginalTitle | 60,231,176 | 0 | 0 |
| title.akas | language | 60,231,176 | 19,619,103 | 32.573 |
| title.akas | ordering | 60,231,176 | 0 | 0 |
| title.akas | region | 60,231,176 | 12,976,473 | 21.544 |
| title.akas | title | 60,231,176 | 0 | 0 |
| title.akas | titleId | 60,231,176 | 0 | 0 |
| title.akas | types | 60,231,176 | 40,273,932 | 66.866 |
| title.basics | endYear | 12,936,379 | 12,772,245 | 98.731 |
| title.basics | genres | 12,936,379 | 547,049 | 4.229 |
| title.basics | isAdult | 12,936,379 | 0 | 0 |
| title.basics | originalTitle | 12,936,379 | 0 | 0 |
| title.basics | primaryTitle | 12,936,379 | 0 | 0 |
| title.basics | runtimeMinutes | 12,936,379 | 8,053,347 | 62.253 |
| title.basics | startYear | 12,936,379 | 1,487,009 | 11.495 |
| title.basics | tconst | 12,936,379 | 0 | 0 |
| title.basics | titleType | 12,936,379 | 0 | 0 |
| title.crew | directors | 12,936,379 | 5,774,164 | 44.635 |
| title.crew | tconst | 12,936,379 | 0 | 0 |
| title.crew | writers | 12,936,379 | 6,415,222 | 49.591 |
| title.episode | episodeNumber | 10,012,173 | 2,112,415 | 21.098 |
| title.episode | parentTconst | 10,012,173 | 0 | 0 |
| title.episode | seasonNumber | 10,012,173 | 2,112,415 | 21.098 |
| title.episode | tconst | 10,012,173 | 0 | 0 |
| title.principals | category | 102,588,083 | 0 | 0 |
| title.principals | characters | 102,588,083 | 52,609,910 | 51.283 |
| title.principals | job | 102,588,083 | 82,747,283 | 80.66 |
| title.principals | nconst | 102,588,083 | 0 | 0 |
| title.principals | ordering | 102,588,083 | 0 | 0 |
| title.principals | tconst | 102,588,083 | 0 | 0 |
| title.ratings | averageRating | 1,729,431 | 0 | 0 |
| title.ratings | numVotes | 1,729,431 | 0 | 0 |
| title.ratings | tconst | 1,729,431 | 0 | 0 |

### Five Most Incomplete Columns

| table | column | missing_pct |
| --- | --- | --- |
| title.akas | attributes | 99.474 |
| title.basics | endYear | 98.731 |
| name.basics | deathYear | 98.321 |
| name.basics | birthYear | 95.661 |
| title.principals | job | 80.66 |

### Detected Anomalies

Checks with at least one occurrence. Orphans are counted after key cleaning.

| table | issue | count | unit | action | example |
| --- | --- | --- | --- | --- | --- |
| title.principals | orphan_nconst_to_name.basics | 7,191 | links | retain_source_require_parent_in_analysis | {"tconst":"tt0022647","ordering":10,"_reference":"nm17757978"} |
| title.crew | orphan_writers_to_name.basics | 1,148 | links | retain_source_require_parent_in_analysis | {"tconst":"tt0090442","_reference":"nm18617482"} |
| title.crew | orphan_directors_to_name.basics | 423 | links | retain_source_require_parent_in_analysis | {"tconst":"tt0850260","_reference":"nm17757903"} |
| name.basics | death_before_birth | 28 | rows | set_null | {"nconst":"nm0012634","deathYear":"456"} |
| name.basics | orphan_knownForTitles_to_title.basics | 8 | links | retain_source_require_parent_in_analysis | {"nconst":"nm18052615","_reference":"tt39377293"} |
| title.episode | orphan_parentTconst_to_title.basics | 5 | links | retain_source_require_parent_in_analysis | {"tconst":"tt28611150","_reference":"tt28606619"} |
| title.basics | invalid_runtimeMinutes | 4 | rows | set_null | {"tconst":"tt0348126","runtimeMinutes":"0"} |
| title.basics | invalid_startYear | 1 | rows | set_null | {"tconst":"tt5174640","startYear":"2115"} |

Checks with zero occurrences on this snapshot:

| table | checks_with_zero_count |
| --- | --- |
| name.basics | conflicting_keys, conflicting_rows, duplicate_rows, invalid_birthYear, invalid_deathYear, lifespan_over_125, missing_or_invalid_key |
| title.akas | conflicting_keys, conflicting_rows, duplicate_rows, invalid_isOriginalTitle, invalid_ordering, missing_or_invalid_key, orphan_titleId_to_title.basics |
| title.basics | conflicting_keys, conflicting_rows, duplicate_rows, end_before_start, invalid_endYear, invalid_isAdult, missing_or_invalid_key |
| title.crew | conflicting_keys, conflicting_rows, duplicate_rows, missing_or_invalid_key, orphan_tconst_to_title.basics |
| title.episode | conflicting_keys, conflicting_rows, duplicate_rows, episode_is_own_parent, invalid_episodeNumber, invalid_seasonNumber, missing_or_invalid_key, orphan_tconst_to_title.basics |
| title.principals | conflicting_keys, conflicting_rows, duplicate_rows, invalid_ordering, missing_or_invalid_key, orphan_tconst_to_title.basics |
| title.ratings | conflicting_keys, conflicting_rows, duplicate_rows, invalid_averageRating, invalid_numVotes, missing_or_invalid_key, orphan_tconst_to_title.basics |

### Applied Treatment

| Action | Effect |
| --- | --- |
| `set_null` | Invalid or inconsistent value replaced by a typed null |
| `exclude_row` | Row without a valid key discarded |
| `collapse_identical_rows` | Identical duplicates merged |
| `exclude_all_variants` | All variants of a conflicting key discarded |
| `retain_source_require_parent_in_analysis` | Orphan link retained, parent required in analytical joins |
| `retain_for_review` | Value retained and flagged |

On this snapshot, no rows were removed: the number of raw and cleaned rows is identical for the seven tables.

### Methodological Explanation

Our data preparation pipeline is designed to be conservative and transparent. Instead of dropping records with quality issues, we prefer to retain them and either null out the invalid fields or flag them for review. 

**Methodology and Choice:**
We normalize missing values by treating nulls, empty strings, and the IMDb-specific marker (`\N`) as equivalent. For numeric fields (like `startYear`, `runtimeMinutes`, or `averageRating`), we use `try_cast` to prevent job failures and set out-of-bounds or unparsable values to `null`. For structural errors (like a director born after they died, or a movie ending before it started), we also null out the incoherent field. For primary keys and foreign keys, if a row has an invalid key format (not matching `tt\d+` or `nm\d+` ...), it is excluded because it cannot be linked to anything else. However, if a foreign key is valid but points to a missing parent record (an orphan), we retain the orphan link in the table but enforce the requirement of a parent record during analytical joins.

**Alternatives Considered:**
- *Dropping rows with nulls*: We rejected this because it would remove a massive portion of the dataset (e.g., `deathYear` is 98% null). Keeping them preserves information for other columns in the same row.
- *Imputing missing values*: Filling missing runtimes with the genre average was rejected because it would fabricate data and skew the exploration results (like Q1, which looks for atypical runtimes).
- *Merging conflicting duplicates*: If two rows share the same key but have different payloads, we discard all variants. Merging them would require arbitrary choices that could bias the dataset.

**Trade-offs:**
By retaining orphans and invalid rows (with nulled fields), we ensure maximum volume for analysis. The trade-off is that analysts must explicitly handle nulls and perform inner joins when a parent record is required, which we document in the project guide.

---

## 2. Data Exploration

Execution thresholds: `PARAMETERS` in `notebooks/02_data_exploration.ipynb`. Full exports are in `data/results/<preparation>/<execution>/`.

### Q1 — Atypical Runtimes by Genre

Movies of type `movie` of at least 40 minutes, genres with at least 30 films, modified z-score `0.6745 × (runtime − median) / MAD` of at least 3.5 in absolute value. Result: 16,401 film–genre pairs for 10,745 distinct films.

**Outliers by genre:**

| genre | outliers | longer | shorter | genre_films | median_min | mad_min | mean_min | sd_min |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Drama | 3,354 | 3,354 | 0 | 196,627 | 93 | 13 | 96 | 37.1 |
| Comedy | 2,706 | 2,706 | 0 | 96,584 | 91 | 10 | 93.1 | 28.7 |
| Action | 1,779 | 1,779 | 0 | 40,345 | 94 | 12 | 101.2 | 33.7 |
| Documentary | 1,314 | 1,314 | 0 | 116,047 | 74 | 16 | 78 | 223.6 |
| Thriller | 1,046 | 1,040 | 6 | 34,075 | 93 | 10 | 96.1 | 21.8 |
| Crime | 1,041 | 1,041 | 0 | 34,618 | 92 | 11 | 95.5 | 25.6 |
| Romance | 987 | 987 | 0 | 42,421 | 95 | 12 | 99.1 | 25.1 |
| Adventure | 802 | 802 | 0 | 24,054 | 90 | 11 | 93 | 30.6 |
| Horror | 660 | 650 | 10 | 30,722 | 88 | 9 | 88.6 | 22.3 |
| Fantasy | 298 | 298 | 0 | 12,714 | 91 | 12 | 93.9 | 31.9 |

**Strongest deviations:**

| tconst | primaryTitle | startYear | genre | runtimeMinutes | robust_z |
| --- | --- | --- | --- | --- | --- |
| tt8273150 | Logistics | 2012 | Documentary | 51,420 | 2,164.6 |
| tt3854496 | Ambiancé | 2020 | Documentary | 43,200 | 1,818 |
| tt12277054 | Carnets Filmés (Liste Complète) | 2019 | Documentary | 28,643 | 1,204.4 |
| tt2659636 | Modern Times Forever | 2011 | Documentary | 14,400 | 603.9 |
| tt2355497 | Beijing 2003 | 2004 | Documentary | 9,000 | 376.3 |

### Q2 — Three Most Popular Genres per Year (1980–2025)

Popularity: sum of IMDb votes for films of the year in the genre (votes in parentheses).

| year | rank_1 | rank_2 | rank_3 |
| --- | --- | --- | --- |
| 1980 | Drama (3,166,513) | Adventure (2,482,053) | Horror (2,072,775) |
| 1990 | Drama (6,326,440) | Comedy (4,019,187) | Crime (3,589,228) |
| 2000 | Drama (13,028,344) | Comedy (8,340,187) | Adventure (6,991,391) |
| 2010 | Drama (17,565,645) | Action (13,337,157) | Adventure (13,178,527) |
| 2020 | Drama (10,480,614) | Comedy (6,515,766) | Action (6,129,695) |
| 2025 | Drama (8,632,927) | Action (7,683,959) | Comedy (5,900,602) |

*(Full table available in the exported Parquet files.)*

### Q3 — Top 3 Genres Compared to the Rest

| year | top3_vote_share | other_genres | top3_votes_per_film | rest_votes_per_film | top3_weighted_rating | rest_weighted_rating |
| --- | --- | --- | --- | --- | --- | --- |
| 1980 | 0.443 | 21 | 3,948 | 2,834 | 7.73 | 7.5 |
| 1990 | 0.456 | 19 | 4,626 | 4,607 | 7.3 | 7.07 |
| 2000 | 0.45 | 21 | 9,026 | 7,940 | 7.21 | 6.98 |
| 2010 | 0.4 | 22 | 6,926 | 4,637 | 6.97 | 7.02 |
| 2020 | 0.448 | 22 | 2,669 | 1,951 | 6.42 | 6.42 |
| 2025 | 0.393 | 22 | 1,980 | 1,917 | 6.51 | 6.46 |

### Q4 — Directors

Exactly one film rated at least 8.0 and at least two other rated films, with an average of at most 6.0. 1,561 directors.

| nconst | primaryName | high_film | high_rating | other_rated_films | other_mean | unrated_films |
| --- | --- | --- | --- | --- | --- | --- |
| nm0002031 | Michael Curtiz | tt0034583 | 8.5 | 170 | 5.68 | 1 |
| nm0064415 | William Beaudine | tt0040488 | 8.2 | 146 | 5.85 | 42 |
| nm0861703 | Richard Thorpe | tt0032138 | 8.1 | 131 | 5.71 | 53 |
| nm0187671 | Alfredo B. Crevenna | tt0284471 | 8.3 | 117 | 5.66 | 32 |
| nm0909825 | Raoul Walsh | tt0042041 | 8.1 | 116 | 5.86 | 6 |

### Q5 — Series

At least three seasons of ten episodes or more, worst episode in the last season, best episode in seasons 1 or 2. 479 series.

| parentTconst | series_title | qualifying_seasons | last_season | episodes | rated_episodes | worst | best |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tt1796154 | Papiny dochki | 20 | 20 | 410 | 259 | 4.6 (S20) | 8.5 (S2) |
| tt6953640 | Roadkill | 13 | 13 | 169 | 9 | 6.8 (S13) | 9.5 (S2) |
| tt1068912 | Ice Road Truckers | 11 | 12 | 154 | 153 | 5.8 (S12) | 9.0 (S1) |
| tt4264964 | Evening Urgant | 11 | 11 | 1,555 | 5 | 7.2 (S11) | 9.4 (S1) |
| tt1563280 | Disappeared | 10 | 11 | 150 | 143 | 5.2 (S11) | 8.6 (S2) |

### Q6 — Success in Several Languages

Success: rating of at least 7.0 and at least 1,000 votes.

*Note: The current output uses the `title_proxy` mode because the `original_languages.csv` file generated by Notebook 3 was not yet available at the time of this exploration run. The `verified` mode is used in the Open Question.*

| successful_films | films_with_language | mode | coverage |
| --- | --- | --- | --- |
| 14,178 | 0 | title_proxy | 0 |

### Q7 — Early-Career Popularity Peak

At least 20 years and 15 films; early career: first five years; ratings with at least 100 votes. 4,573 actors and actresses.

| nconst | primaryName | first_year | last_year | dated_films | early_peak_film | peak_year | early_peak_rating | later_peak_rating |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm0006669 | William Sadler | 1990 | 2025 | 55 | tt0111161 | 1994 | 9.3 | 7.4 |
| nm0001570 | Edward Norton | 1996 | 2026 | 32 | tt0137523 | 1999 | 8.8 | 8.1 |
| nm0000641 | Gary Sinise | 1992 | 2026 | 30 | tt0109830 | 1994 | 8.8 | 8.2 |
| nm0482851 | Phil LaMarr | 1994 | 2026 | 23 | tt0110912 | 1994 | 8.8 | 7.1 |
| nm0000199 | Al Pacino | 1971 | 2026 | 60 | tt0068646 | 1972 | 9.2 | 8.3 |

### Q8 — Professionals with Multiple Roles

381,967 professionals with at least two professions over their entire career. Summary: for each person, average of multi-role films minus average of single-role films (films with at least 100 votes).

| combination | people | multi_rated_films | single_rated_films | mean_multi_rating | mean_single_rating | mean_rating_delta | median_rating_delta | share_better_multi | share_worse_multi |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| any | 42,618 | 104,392 | 313,054 | 5.707 | 5.729 | -0.022 | 0 | 0.49 | 0.499 |
| director+writer | 20,886 | 54,476 | 113,126 | 5.825 | 5.836 | -0.011 | 0 | 0.492 | 0.497 |
| actor+writer | 5,866 | 8,528 | 62,072 | 5.516 | 5.559 | -0.042 | 0 | 0.493 | 0.497 |
| actor+director+writer | 3,892 | 6,408 | 41,655 | 5.479 | 5.56 | -0.081 | -0.05 | 0.474 | 0.517 |
| actor+director | 1,446 | 2,012 | 26,723 | 5.553 | 5.705 | -0.151 | -0.119 | 0.443 | 0.548 |

### Methodological Explanation

The exploration phase aims to answer eight distinct questions using PySpark, prioritizing correctness and explicit assumptions over black-box results.

**Methodology and Choice:**
- **Q1 (Runtimes)**: We used a modified z-score based on the Median Absolute Deviation (MAD) instead of the standard deviation. This makes the detection of outliers robust to extreme values (like 50,000-minute experimental films) that would otherwise skew the mean and standard deviation.
- **Q2 & Q3 (Genres)**: We defined "popularity" as the sum of IMDb votes. This reflects actual audience engagement rather than just the volume of films produced. Votes are a better proxy for cultural impact than the number of releases.
- **Q6 (Languages)**: The public IMDb dataset does not contain the spoken languages of a film. To overcome this, we introduced two modes. The `title_proxy` mode uses the language of the original title from `title.akas`. The `verified` mode (used in our final report) relies on Wikidata (P364) via a generated CSV file. This ensures we only analyze documented multilingualism, keeping provenance for every record.
- **Q7 (Career Peaks)**: We defined "early career" as the first five calendar years after the actor's first credited film. We required a minimum of 100 votes per film to filter out obscure productions.

**Alternatives Considered:**
- For Q1, we considered using the Interquartile Range (IQR), but MAD is generally better for heavily skewed distributions like film runtimes.
- For Q2, we could have counted the number of films per genre, but this favors genres with high production volume (like Adult or Documentary) rather than genres with high audience reach (like Action or Drama).

**Trade-offs:**
Using a robust z-score means we are more conservative in flagging outliers. Using votes instead of film counts favors mainstream blockbusters, which aligns with our definition of "popularity".

---

## 3. Data Enrichment

Movies of type `movie` from 1980 to 2025, at least 40 minutes; the 10 most voted films per genre and per year; the top 3 principals of each film.

| Metric | Value |
| --- | --- |
| Film–genre–year rows | 10,198 |
| Distinct films | 7,186 |
| Film–person principals | 21,493 |
| Distinct people | 11,283 |

### Film Coverage (Synopsis)

| enrichment_status | enrichment_note | count | pct |
| --- | --- | --- | --- |
| enriched | | 6,000 | 83.50 |
| not_available | No Wikidata item carries this IMDb identifier (P345) | 490 | 6.82 |
| not_available | Wikipedia article has no plot or synopsis section | 368 | 5.12 |
| not_available | Wikidata item has no English Wikipedia article | 328 | 4.56 |

### Principal Coverage (Biography)

| enrichment_status | enrichment_note | count | pct |
| --- | --- | --- | --- |
| enriched | | 18,120 | 84.31 |
| not_available | No Wikidata item carries this IMDb identifier (P345) | 2,111 | 9.82 |
| not_available | Wikidata item has no English Wikipedia article | 1,248 | 5.81 |
| not_available | Not looked up yet: gathering was limited or interrupted | 14 | 0.07 |

Each record carries `source_url` and, if an article was read, `source_revision_url` (revision permalink).

### Methodological Explanation

The enrichment phase adds synopses and biographies to our top films and principals, strictly respecting the project rules on open sources and provenance.

**Methodology and Choice:**
We used Wikidata as a bridge. We queried Wikidata via its SPARQL endpoint using the IMDb ID (P345) to find the corresponding English Wikipedia article URL. Once the URL was found, we fetched the Wikipedia page via the MediaWiki API. For films, we extracted the "Plot" or "Synopsis" section. For people, we extracted the introduction (bio summary). Every enriched record contains a `source_url` and a `source_revision_url` (a permalink to the specific revision), ensuring perfect data lineage. We implemented a `PoliteClient` with a 0.2-second delay between requests and an explicit contact email in the User-Agent to strictly respect Wikimedia's API rate limits.

**Alternatives Considered:**
- *Scraping Wikipedia directly without Wikidata*: Wikipedia titles are not always the same as the IMDb primary title (e.g., "The Dark Knight" vs "The Dark Knight (film)"). Wikidata guarantees an exact match via the IMDb ID.
- *Using TMDB or OMDb APIs*: These require paid API keys, and their terms of service might not align with the project's open-source requirement. Wikipedia is human-authored and freely accessible.
- *Using a Python UDF for parsing*: We used Spark's native string functions and structs to extract the sections, ensuring the pipeline scales to the full IMDb dataset without memory issues.

**Trade-offs:**
Wikipedia does not always have a "Plot" section, or the English Wikipedia article might not exist (even if a Wikidata item does). In those cases, we programmatically add a note (e.g., `enrichment_status = not_available`) rather than guessing or fabricating data.

---

## 4. Streaming Data Processing

The pipeline ingests live Wikimedia change events via Kafka and processes them with Spark Structured Streaming.

### Results

**Total events captured:** 178 rows.

**Events by wiki and change type:**

| wiki | type | count |
| --- | --- | --- |
| enwiki | edit | 68 |
| wikidatawiki | edit | 50 |
| commonswiki | categorize | 17 |
| enwiki | categorize | 14 |
| enwiki | log | 11 |
| frwiki | edit | 6 |
| itwiki | edit | 5 |
| commonswiki | edit | 5 |
| dewiki | edit | 4 |
| enwiki | new | 3 |
| trwiki | categorize | 2 |
| eswiki | edit | 2 |
| itwiki | categorize | 2 |
| cywiki | categorize | 2 |
| wikidatawiki | new | 2 |
| eswiki | new | 1 |
| cywiki | log | 1 |
| trwiki | new | 1 |
| commonswiki | log | 1 |
| frwiki | categorize | 1 |

**Sample of recent matched titles:**

| title | type | event_time |
| --- | --- | --- |
| Q21775771 | edit | 2026-11-09 21:10:39 |
| Q6978549 | edit | 2026-11-09 21:10:34 |
| Talk:List of accolades received by Inside Out (2015 film) | log | 2026-11-09 21:10:33 |
| Category:Redirects from moves | categorize | 2026-11-09 21:10:33 |
| List of accolades received by Inside Out (2015 film) | log | 2026-11-09 21:10:32 |
| Talk:List of accolades received by Inside Out | log | 2026-11-09 21:10:32 |
| Category:Redirects from moves | categorize | 2026-11-09 21:10:32 |
| Category:Redirects connected to a Wikidata item | categorize | 2026-11-09 21:10:32 |
| List of accolades received by Inside Out | log | 2026-11-09 21:10:31 |
| Mickey Lemle | edit | 2026-11-09 21:10:24 |

### Methodological Explanation

The streaming pipeline ingests live Wikimedia change events via Kafka and processes them with Spark Structured Streaming.

**Methodology and Choice:**
We used a Python producer to connect to the Wikimedia EventStreams SSE feed. The producer filters events based on a set of movie-related keywords (`film`, `movie`, `actor`, `director`, etc.). The filter is intentionally broad to guarantee a sufficient volume of events during a 15-30 minute test window. The filtered events are serialized to JSON and published to a Kafka topic (`wiki-changes`). Spark Structured Streaming consumes this topic, parses the JSON, and writes the results to Parquet. To handle network disconnections, the producer is wrapped in a reconnection loop with exponential backoff, ensuring continuous ingestion.

**Alternatives Considered:**
- *Filtering strictly on IMDb IDs*: The Wikipedia change event stream does not contain IMDb IDs. Filtering on the exact title string would be too restrictive and would yield zero events.
- *Using a stateful aggregation (e.g., `groupBy().count()`)*: While possible, our goal was to store the raw filtered events for later analysis (e.g., tracking which pages are most edited). We use `append` mode to simply archive the valid events.

**Trade-offs:**
The broad keyword filter introduces some false positives (e.g., non-English categories). However, it ensures we satisfy the project requirement of capturing multiple relevant events within the test window.

### Late-Arriving Data Explanation

To handle late-arriving data, we use **event-time processing** with a **10-minute watermark** on the `event_time` column (derived from the Wikimedia event timestamp). 

- **Why a watermark?** Wikimedia is a distributed system; edits can arrive out of order. Without a watermark, Spark would have to keep all state in memory indefinitely, leading to OutOfMemory errors. The watermark tells Spark to wait up to 10 minutes for late events before considering the window closed.
- **What happens to late data?** Events that arrive more than 10 minutes after the current maximum event time are considered "too late" and are dropped from stateful aggregations. Given the low latency of the producer (seconds), a 10-minute window is highly tolerant.
- **Alternatives considered:** We rejected a 30-minute or 1-hour watermark because it would increase memory usage without adding value for a short demo. We also rejected using `processingTime` only, as it ignores the actual event time and prevents stateful operations.

---

## 5. Open Question

### Question, Interest, and Data Used

**Question:** Does being a multilingual, non-Anglophone actor correlate with higher IMDb ratings or longer careers?

**Interest:** Hollywood has historically been criticized for its Anglo-centric bias. However, globalization has allowed non-Anglophone actors to cross over into English-language cinema. We wanted to quantify whether this multilingualism (acting in films with different original languages) translates into better critical reception (ratings) or longer career longevity. 

**Additional Data Required:**
- We used the **enriched Wikipedia biographies** (from Notebook 3) to extract the country of birth.
- We used the **Wikidata original languages** (P364) to count the number of distinct languages per actor.

### Results

Actors and actresses with at least 5 rated films, with an identified nationality.

| is_multilingual | num_actors | avg_imdb_rating | avg_career_years | avg_films |
| --- | --- | --- | --- | --- |
| Monolingual | 4,010 | 5.88 | 28.9 | 27.3 |
| Multilingual | 1,741 | 6.14 | 33.3 | 44.7 |

| is_anglophone | is_multilingual | num_actors | avg_imdb_rating | avg_career_years |
| --- | --- | --- | --- | --- |
| Anglophone | Monolingual | 2,840 | 5.76 | 28.0 |
| Anglophone | Multilingual | 1,066 | 6.11 | 32.5 |
| Non-Anglophone | Monolingual | 1,170 | 6.17 | 31.2 |
| Non-Anglophone | Multilingual | 675 | 6.19 | 34.4 |

| country | num_actors | avg_imdb_rating | avg_career_years |
| --- | --- | --- | --- |
| Japan | 270 | 6.66 | 30.0 |
| Germany | 114 | 6.31 | 31.7 |
| Brazil | 42 | 6.30 | 34.3 |
| Argentina | 26 | 6.26 | 35.3 |
| Ireland | 71 | 6.19 | 26.9 |
| Russia | 93 | 6.17 | 33.9 |
| China | 56 | 6.17 | 29.1 |
| Norway | 29 | 6.16 | 31.7 |
| India | 490 | 6.13 | 28.9 |
| United Kingdom | 699 | 6.12 | 28.5 |

### Key Takeaways

- **Multilingual actors** have a higher average IMDb rating (6.14) compared to monolingual actors (5.88). They also have significantly longer careers (33.3 years vs 28.9 years) and appear in more films on average (44.7 vs 27.3).
- **Anglophone vs. Non-Anglophone**: Non-Anglophone actors generally achieve higher average ratings than Anglophone actors, regardless of whether they are monolingual or multilingual.
- **Top Countries**: Actors born in Japan have the highest average IMDb rating (6.66), followed by Germany and Brazil.
- **Limitations**: The country extraction relies on a heuristic on the first 300 characters of the biography. Some actors may be misclassified. The multilingualism proxy uses the original language of the films, not the actor's actual spoken languages.

### Methodological Explanation

**Methodology and Choice:**
We combined IMDb credits (`title.principals`), film ratings, and original languages. For each actor, we calculated:
1. The number of distinct original languages in their filmography.
2. Their average IMDb rating (for films with ≥100 votes).
3. Their career length (last year − first year).
We extracted the country of birth by searching for nationality adjectives (e.g., "American", "French") in the first 300 characters of the Wikipedia biography. We classified actors as "Multilingual" if they acted in at least 2 distinct original languages. We filtered out actors with fewer than 5 films or an unknown country to avoid noise.

**Alternatives Considered:**
- *Using `title.akas` region*: This field indicates where a film was released, not where the actor was born. An American actor can be in a film released in France without being French.
- *Using Python UDFs for country extraction*: Our first attempt used a Python UDF, but it caused `Python worker failed to connect back` errors on Windows. We replaced it with native Spark expressions (`F.when`), which are more stable and faster.
- *Using the actor's spoken languages*: This data is not available in IMDb. The original language of the films is a solid proxy for the actor's linguistic range.

**Trade-offs:**
The country extraction is a heuristic; some actors may be misclassified (e.g., if their biography starts with a different phrasing). The multilingualism proxy measures the languages of the films, not the languages the actor actually speaks. Despite these limitations, the large sample size (5,751 actors) provides statistically significant trends.