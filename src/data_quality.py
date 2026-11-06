"""Spark quality metrics and conservative cleaning for the IMDb snapshot."""

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import Observation, functions as F

from src.imdb_loader import COLUMNS, download_table, inflate_tsv, read_prepared, read_tsv

KEYS = {
    "title.basics": ("tconst",),
    "name.basics": ("nconst",),
    "title.ratings": ("tconst",),
    "title.crew": ("tconst",),
    "title.episode": ("tconst",),
    "title.principals": ("tconst", "ordering"),
    "title.akas": ("titleId", "ordering"),
}

REFERENCES = (
    ("title.akas", "titleId", "title.basics", "tconst", False),
    ("title.ratings", "tconst", "title.basics", "tconst", False),
    ("title.crew", "tconst", "title.basics", "tconst", False),
    ("title.crew", "directors", "name.basics", "nconst", True),
    ("title.crew", "writers", "name.basics", "nconst", True),
    ("title.episode", "tconst", "title.basics", "tconst", False),
    ("title.episode", "parentTconst", "title.basics", "tconst", False),
    ("title.principals", "tconst", "title.basics", "tconst", False),
    ("title.principals", "nconst", "name.basics", "nconst", False),
    ("name.basics", "knownForTitles", "title.basics", "tconst", True),
)


def missing(column):
    return column.isNull() | F.trim(column).isin("", r"\N")


def _numeric_rules(table, year):
    return {
        "name.basics": {"birthYear": ("int", 1, year), "deathYear": ("int", 1, year)},
        "title.basics": {
            "isAdult": ("int", 0, 1),
            "startYear": ("int", 1870, year + 10),
            "endYear": ("int", 1870, year + 10),
            "runtimeMinutes": ("int", 1, 2**31 - 1),
        },
        "title.ratings": {
            "averageRating": ("double", 1, 10),
            "numVotes": ("bigint", 1, 2**63 - 1),
        },
        "title.episode": {
            "seasonNumber": ("int", 0, 2**31 - 1),
            "episodeNumber": ("int", 0, 2**31 - 1),
        },
        "title.principals": {"ordering": ("int", 1, 2**31 - 1)},
        "title.akas": {"ordering": ("int", 1, 2**31 - 1), "isOriginalTitle": ("int", 0, 1)},
    }.get(table, {})


def cleaning_rules(table, raw, year):
    """Return normalized column expressions and row-level audit predicates."""
    columns = {name: F.when(~missing(F.col(name)), F.col(name)) for name in raw.columns}
    issues = {}
    for name, (dtype, lower, upper) in _numeric_rules(table, year).items():
        value = F.expr(f"try_cast(trim(`{name}`) as {dtype})")
        valid = value.isNotNull() & value.between(lower, upper)
        issues[f"invalid_{name}"] = (~missing(F.col(name)) & ~valid, name, "set_null")
        columns[name] = F.when(valid, value)

    if table == "name.basics":
        birth, death = columns["birthYear"], columns["deathYear"]
        reversed_years = death < birth
        issues["death_before_birth"] = (reversed_years, "deathYear", "set_null")
        issues["lifespan_over_125"] = (death - birth > 125, "deathYear", "retain_for_review")
        columns["deathYear"] = F.when(~F.coalesce(reversed_years, F.lit(False)), death)
    if table == "title.basics":
        reversed_years = columns["endYear"] < columns["startYear"]
        issues["end_before_start"] = (reversed_years, "endYear", "set_null")
        columns["endYear"] = F.when(~F.coalesce(reversed_years, F.lit(False)), columns["endYear"])
    if table == "title.episode":
        self_parent = columns["parentTconst"] == columns["tconst"]
        issues["episode_is_own_parent"] = (self_parent, "parentTconst", "set_null")
        columns["parentTconst"] = F.when(~F.coalesce(self_parent, F.lit(False)), columns["parentTconst"])

    key = KEYS[table][0]
    pattern = r"^nm[0-9]+$" if key == "nconst" else r"^tt[0-9]+$"
    invalid_key = ~F.coalesce(columns[key].rlike(pattern), F.lit(False))
    if "ordering" in KEYS[table]:
        invalid_key = invalid_key | columns["ordering"].isNull()
    issues["missing_or_invalid_key"] = (invalid_key, key, "exclude_row")
    return columns, issues


def _sample(table, column):
    names = list(dict.fromkeys((*KEYS[table], column)))
    return F.to_json(F.struct(*[F.substring(F.col(name), 1, 120).alias(name) for name in names]))


def _profile(table, raw, output, year):
    """Audit and clean a table while restricting wide comparisons to repeated keys."""
    columns, issues = cleaning_rules(table, raw, year)
    before, duplicates, after = Observation(), Observation(), Observation()
    observed = raw.observe(
        before,
        F.count("*").alias("rows"),
        *[F.count_if(missing(F.col(name))).alias(name) for name in columns],
        *[
            metric
            for issue, (condition, column, _) in issues.items()
            for metric in (
                F.count_if(condition).alias(f"count_{issue}"),
                F.min(F.when(condition, _sample(table, column))).alias(f"example_{issue}"),
            )
        ],
    ).persist(StorageLevel.DISK_ONLY)
    keys = KEYS[table]
    valid = observed.filter(~issues["missing_or_invalid_key"][0])
    # Ordering is an integer key. Compare the other fields as raw text so
    # cleaning cannot turn two conflicting variants into an identical row.
    payload = [name for name in columns if name not in keys]
    keyed = valid.select(
        *[columns[name].alias(name) for name in keys],
        *[F.col(name) for name in payload],
    )
    repeated_keys = keyed.groupBy(*keys).count().filter(F.col("count") > 1).drop("count")
    repeated_keys = repeated_keys.persist(StorageLevel.DISK_ONLY)
    try:
        has_duplicates = repeated_keys.count() > 0
        unique = keyed.join(repeated_keys, list(keys), "left_anti") if has_duplicates else keyed
        clean = unique.select(*[F.col(name) if name in keys else expr.alias(name)
                                for name, expr in columns.items()])
        if has_duplicates:
            variants = F.struct(*[F.when(~missing(F.col(name)), F.col(name)).alias(name)
                                  for name in payload])
            grouped = keyed.join(repeated_keys, list(keys), "left_semi").groupBy(*keys).agg(
                F.min(variants).alias("_first"), F.max(variants).alias("_last"), F.count("*").alias("_rows")
            )
            conflict = F.col("_first") != F.col("_last")
            grouped = grouped.observe(
                duplicates,
                F.count_if(conflict).alias("conflicting_keys"),
                F.sum(F.when(conflict, F.col("_rows")).otherwise(0)).alias("conflicting_rows"),
                F.sum(F.when(~conflict, F.col("_rows") - 1).otherwise(0)).alias("duplicate_rows"),
                F.min(F.when(conflict, F.to_json(F.struct(*keys, "_first", "_last")))).alias("conflict_example"),
                F.min(F.when(~conflict, F.to_json(F.struct(*keys, "_rows")))).alias("duplicate_example"),
            )
            resolved = grouped.filter(~conflict).select(*keys, "_first.*").select(
                *[F.col(name) if name in keys else expr.alias(name) for name, expr in columns.items()]
            )
            clean = clean.unionByName(resolved)
        clean.observe(
            after, F.count("*").alias("rows"),
            *[F.count_if(F.col(name).isNull()).alias(name) for name in columns],
        ).write.parquet(output.as_posix())
    finally:
        repeated_keys.unpersist()
        observed.unpersist()

    profiles = [(table, name, stage, counts["rows"], counts[name],
                 100.0 * counts[name] / counts["rows"] if counts["rows"] else None)
                for stage, counts in (("raw", before.get), ("clean", after.get)) for name in columns]
    summary = [(table, issue, before.get[f"count_{issue}"], "rows", action,
                before.get[f"example_{issue}"])
               for issue, (_, _, action) in issues.items()]
    metrics = duplicates.get if has_duplicates else {
        "duplicate_rows": 0, "conflicting_keys": 0, "conflicting_rows": 0,
        "duplicate_example": None, "conflict_example": None,
    }
    summary.extend([
        (table, "duplicate_rows", metrics["duplicate_rows"] or 0, "rows",
         "collapse_identical_rows", metrics["duplicate_example"]),
        (table, "conflicting_keys", metrics["conflicting_keys"], "keys",
         "exclude_all_variants", metrics["conflict_example"]),
        (table, "conflicting_rows", metrics["conflicting_rows"] or 0, "rows",
         "exclude_all_variants", metrics["conflict_example"]),
    ])
    return profiles, summary


def reference_audit(tables):
    """Count remaining orphan links on usable keys, with one example per relation."""
    for table, column, parent, parent_key, is_array in REFERENCES:
        references = F.explode(F.split(F.col(column), ",")) if is_array else F.col(column)
        links = tables[table].select(*KEYS[table], references.alias("_reference"))
        links = links.withColumn("_reference", F.trim("_reference")).filter(F.col("_reference") != "")
        parents = tables[parent].select(F.col(parent_key).alias("_reference"))
        orphan = links.join(parents, "_reference", "left_anti").agg(
            F.count("*").alias("n"),
            F.min(F.to_json(F.struct(*KEYS[table], "_reference"))).alias("example"),
        ).first()
        yield (table, f"orphan_{column}_to_{parent}", orphan.n, "links",
               "retain_source_require_parent_in_analysis", orphan.example)


def prepare(spark, root, *, expand_gzip=True, year=None):
    """Publish all clean tables and audit outputs together after a successful run."""
    year = date.today().year if year is None else year
    files = {table: download_table(table, root / "raw") for table in COLUMNS}
    run = uuid4().hex
    processed = root / "processed"
    snapshot = processed / run
    snapshot.mkdir(parents=True)
    profiles, issues = [], []
    with TemporaryDirectory(prefix="imdb-tsv-", dir=os.getenv("IMDB_SCRATCH_DIR")) as scratch:
        for table, archive in files.items():
            print(f"Préparation de {table}", flush=True)
            path = inflate_tsv(archive, Path(scratch) / f"{table}.tsv") if expand_gzip else archive
            table_profiles, table_issues = _profile(
                table, read_tsv(spark, table, path), snapshot / "tables" / table, year
            )
            if expand_gzip:
                path.unlink()
            profiles.extend(table_profiles)
            issues.extend(table_issues)

    print("Contrôle des clés étrangères", flush=True)
    issues.extend(reference_audit(read_prepared(spark, snapshot)))
    profile = spark.createDataFrame(
        profiles, "table string, column string, stage string, rows long, missing long, missing_pct double"
    )
    anomalies = spark.createDataFrame(
        issues, "table string, issue string, count long, unit string, action string, example string"
    )
    profile.coalesce(1).write.parquet((snapshot / "quality" / "missing").as_posix())
    anomalies.coalesce(1).write.parquet((snapshot / "quality" / "anomalies").as_posix())
    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "validation_year": year,
        "sources": {path.name: {"bytes": path.stat().st_size, "mtime": path.stat().st_mtime}
                    for path in files.values()},
    }
    (snapshot / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    pointer = processed / f".{run}.latest"
    pointer.write_text(run + "\n", encoding="utf-8")
    pointer.replace(processed / "LATEST")
    return snapshot
