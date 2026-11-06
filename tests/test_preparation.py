import gzip

import pytest
from pyspark.sql import types as T

from src.data_quality import _profile, cleaning_rules, reference_audit
from src.imdb_loader import download_table, read_tsv


def test_profile_reads_gzip_and_audits_invalid_and_duplicate_keys(spark, tmp_path):
    raw_dir = tmp_path / "raw data & sources"
    raw_dir.mkdir()
    archive = raw_dir / "name.basics.tsv.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as stream:
        stream.write("nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles\n")
        stream.write("nm1\tAlice\t1990\t1980\tactor\t\\N\n" * 2)
        stream.write("nm2\tBob\tunknown\t\\N\t   \t\n")
        stream.write("nm3\tCarol\t1985\t\\N\tactor\t\\N\n")
        stream.write("nm3\tDifferent\t1985\t\\N\tactor\t\\N\n")
        stream.write("\\N\tNo key\t1980\t\\N\tactor\t\\N\n")

    assert download_table("name.basics", raw_dir) == archive
    raw = read_tsv(spark, "name.basics", archive)
    out = tmp_path / "clean data & results"
    profiles, issues = _profile("name.basics", raw, out, 2026)
    stats = {(stage, name): (total, missing, pct)
             for _, name, stage, total, missing, pct in profiles}
    counts = {name: count for _, name, count, *_ in issues}
    assert stats["raw", "knownForTitles"] == (6, 6, 100.0)
    assert stats["raw", "primaryProfession"][:2] == (6, 1)
    assert stats["clean", "birthYear"] == (2, 1, 50.0)
    assert stats["clean", "deathYear"] == (2, 2, 100.0)
    assert counts["death_before_birth"] == 2
    assert counts["invalid_birthYear"] == 1
    assert counts["duplicate_rows"] == counts["missing_or_invalid_key"] == 1
    assert counts["conflicting_keys"] == 1 and counts["conflicting_rows"] == 2

    assert (out / "_SUCCESS").is_file()
    clean = spark.read.parquet(out.as_posix())
    assert clean.schema["birthYear"].dataType == T.IntegerType()
    assert [(row.nconst, row.birthYear) for row in clean.orderBy("nconst").collect()] == [
        ("nm1", 1990), ("nm2", None),
    ]


def test_rating_validation_handles_nan_and_overflow(spark):
    raw = spark.createDataFrame([
        ("tt1", "8.2", "2147483648"),
        ("tt2", "NaN", "9223372036854775808"),
        ("tt3", "11.0", "-1"),
    ], "tconst string, averageRating string, numVotes string")
    columns, _ = cleaning_rules("title.ratings", raw, 2026)
    clean = raw.select(*[value.alias(name) for name, value in columns.items()])
    assert clean.schema["numVotes"].dataType == T.LongType()
    assert [(row.averageRating, row.numVotes) for row in clean.orderBy("tconst").collect()] == [
        (8.2, 2147483648), (None, None), (None, None),
    ]


@pytest.mark.parametrize("rows", [[], [("tt1", "8.0", "10")]])
def test_profile_handles_tables_without_repeated_keys(spark, tmp_path, rows):
    raw = spark.createDataFrame(rows, "tconst string, averageRating string, numVotes string")
    profiles, issues = _profile("title.ratings", raw, tmp_path / "clean", 2026)
    assert all(total == len(rows) and missing == 0 for _, _, _, total, missing, _ in profiles)
    assert all(pct == (0.0 if rows else None) for *_, pct in profiles)
    assert all(count == 0 for _, _, count, *_ in issues)


def test_reference_audit_counts_scalar_and_list_links(spark):
    specs = {
        "title.basics": ([("tt1",)], "tconst string"),
        "name.basics": ([("nm1", "tt1,tt7")], "nconst string, knownForTitles string"),
        "title.ratings": ([("tt2",)], "tconst string"),
        "title.crew": ([("tt1", "nm1,nm2", None)], "tconst string, directors string, writers string"),
        "title.episode": ([("tt1", None)], "tconst string, parentTconst string"),
        "title.principals": ([("tt1", 1, "nm1")], "tconst string, ordering int, nconst string"),
        "title.akas": ([("tt1", 1)], "titleId string, ordering int"),
    }
    tables = {name: spark.createDataFrame(rows, schema) for name, (rows, schema) in specs.items()}
    observed = {(table, issue): count for table, issue, count, *_ in reference_audit(tables)}
    assert {key: count for key, count in observed.items() if count} == {
        ("title.ratings", "orphan_tconst_to_title.basics"): 1,
        ("title.crew", "orphan_directors_to_name.basics"): 1,
        ("name.basics", "orphan_knownForTitles_to_title.basics"): 1,
    }
