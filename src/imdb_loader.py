"""Local Spark configuration, public IMDb inputs and prepared snapshots."""

import gzip
import os
import shutil
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

DATA_URL = "https://datasets.imdbws.com/"
COLUMNS = {
    "title.basics": "tconst titleType primaryTitle originalTitle isAdult startYear "
    "endYear runtimeMinutes genres",
    "name.basics": "nconst primaryName birthYear deathYear primaryProfession knownForTitles",
    "title.ratings": "tconst averageRating numVotes",
    "title.crew": "tconst directors writers",
    "title.episode": "tconst parentTconst seasonNumber episodeNumber",
    "title.principals": "tconst ordering nconst category job characters",
    "title.akas": "titleId ordering title region language types attributes isOriginalTitle",
}


def data_root(project_root):
    """Resolve a shared data directory from the repository root."""
    path = Path(os.getenv("IMDB_DATA_DIR", "data")).expanduser()
    return (project_root / path).resolve()


def spark_session(app_name):
    threads = int(os.getenv("IMDB_SPARK_THREADS", "4"))
    partitions = int(os.getenv("IMDB_SPARK_PARTITIONS", str(threads * 8)))
    if min(threads, partitions) < 1:
        raise ValueError("IMDB_SPARK_THREADS et IMDB_SPARK_PARTITIONS doivent être positifs.")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master(f"local[{threads}]").appName(app_name)
        .config("spark.driver.memory", os.getenv("IMDB_SPARK_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def download_table(table, raw_dir):
    """Reuse an existing archive or publish a complete download atomically."""
    if table not in COLUMNS:
        raise ValueError(f"Table IMDb inconnue : {table}")
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{table}.tsv.gz"
    if path.is_file() and path.stat().st_size:
        return path

    from tempfile import NamedTemporaryFile

    temporary = None
    try:
        with NamedTemporaryFile(dir=raw_dir, suffix=".part", delete=False) as output:
            temporary = Path(output.name)
            with urlopen(DATA_URL + path.name, timeout=60) as response:
                shutil.copyfileobj(response, output, length=1024 * 1024)
                expected_size = response.headers.get("Content-Length")
                if expected_size and output.tell() != int(expected_size):
                    raise OSError("Archive reçue incomplète.")
        temporary.replace(path)
    except (OSError, URLError) as exc:
        raise RuntimeError(f"Téléchargement impossible pour {path.name}. "
                           f"Placez l’archive officielle dans {raw_dir}.") from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return path


def read_tsv(spark, table, path):
    """Keep numeric fields as text until the quality audit checks their tokens."""
    schema = StructType([StructField(column, StringType()) for column in COLUMNS[table].split()])
    return (
        spark.read.schema(schema)
        .options(sep="\t", header="true", nullValue=r"\N", quote="\u0000",
                 encoding="UTF-8", mode="FAILFAST", enforceSchema="false")
        .csv(path.resolve().as_uri())
    )


def inflate_tsv(archive, destination):
    """Stream one archive to a scratch TSV so Spark can split large inputs."""
    with gzip.open(archive, "rb") as source, destination.open("wb") as output:
        shutil.copyfileobj(source, output, length=1024 * 1024)
    return destination


def prepared_snapshot(root):
    """Resolve the last fully published run once for all downstream reads."""
    pointer = root / "processed" / "LATEST"
    if not pointer.is_file():
        raise FileNotFoundError("Exécutez 01_data_preparation.ipynb avant l’exploration.")
    run = pointer.read_text(encoding="utf-8").strip()
    if len(run) != 32 or any(char not in "0123456789abcdef" for char in run):
        raise ValueError(f"Identifiant de préparation invalide dans {pointer}.")
    snapshot = pointer.parent / run
    if not (snapshot / "manifest.json").is_file():
        raise FileNotFoundError(f"Préparation incomplète : {snapshot}")
    return snapshot


def read_prepared(spark, snapshot):
    return {
        table: spark.read.parquet((snapshot / "tables" / table).as_uri())
        for table in COLUMNS
    }
