
# ECE Big Data Processing Project · IMDb

## Contributors

- [Name 1] - [email1@ece.fr]
- Anas BOUTALEB - [anas.boutaleb@edu.ece.fr]
- [Name 3] - [email3@ece.fr]

## Installation and Setup

The complete local processing requires Python 3.11 or 3.12, Java 17, HTTPS access to PyPI and IMDb, 8 GB of RAM, and about 20 GB of free disk space for the archives, Spark work, and one preparation snapshot.

Java 17 must be accessible via `JAVA_HOME` or `PATH`.

### 1. Python Environment (Linux / macOS / WSL)

On Ubuntu or Ubuntu WSL, install the prerequisites from the Linux terminal:

```bash
sudo apt-get update
sudo apt-get install openjdk-17-jdk python3 python3-venv
```

From the repository root, create the Python environment and launch Jupyter:

```bash
java -version
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m ipykernel install --prefix "$VIRTUAL_ENV" --name imdb --display-name "Python (IMDb)"
python -m pytest -q
jupyter lab
```

Open `notebooks/01_data_preparation.ipynb` with the Python (IMDb) kernel and run all cells. Repeat with `notebooks/02_data_exploration.ipynb`.

Execution can also be done headless from the root. The executed copies will be in `/tmp`:

```bash
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=imdb \
  --ExecutePreprocessor.timeout=-1 --output-dir=/tmp notebooks/01_data_preparation.ipynb

jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=imdb \
  --ExecutePreprocessor.timeout=-1 --output-dir=/tmp notebooks/02_data_exploration.ipynb
```

### 2. Windows Setup: Hadoop / winutils Prerequisites

PySpark relies on Hadoop binaries (notably `winutils.exe` and `hadoop.dll`) to perform filesystem operations on Windows.

Without them, writing or reading Parquet files (including the data preparation step) fails with:

```text
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
```

Follow the steps below before running any notebook or streaming job.

#### Prerequisites for Windows

- Windows 10 or Windows 11.
- PowerShell (Administrator mode recommended for the environment variables).
- Java 17 or 21 installed and available on PATH (`java -version` must succeed).

#### Install winutils and configure the environment

Open PowerShell, preferably as Administrator, and run the following commands. They download the Hadoop 3.4.0 Windows binaries, place them in `C:\hadoop\bin`, and register the required environment variables.

```powershell
# Create the target directory
New-Item -ItemType Directory -Force -Path C:\hadoop\bin

# Download the winutils bundle
$url = "https://github.com/kontext-tech/winutils/archive/refs/heads/master.zip"
Invoke-WebRequest $url -OutFile "$env:TEMP\winutils.zip"

# Extract and copy the Hadoop 3.4.0 Windows binaries
Expand-Archive "$env:TEMP\winutils.zip" "$env:TEMP\winutils" -Force
Copy-Item "$env:TEMP\winutils\winutils-master\hadoop-3.4.0-win10-x64\bin\*" "C:\hadoop\bin\" -Recurse -Force

# Register HADOOP_HOME for the current user
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")

# Add C:\hadoop\bin to the user PATH (only if not already present)
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*C:\hadoop\bin*") {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;C:\hadoop\bin", "User")
}

# Register HADOOP_HOME at machine scope (requires Administrator)
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "Machine")
```

After running these commands, close all terminals and open a new one for the changes to take effect.

Then, create the Python virtual environment in PowerShell (from the project root):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Streaming Pipeline Setup (Kafka & Docker)

The streaming pipeline requires a running Kafka broker.

For local execution, a single-node broker is provided through `docker-compose.yml`.

This is an implementation choice for reproducibility: the project specification requires Kafka, but does not prescribe how to install it.

Any Kafka-compatible broker listening on `localhost:9092` can be used instead.

#### Start Kafka

```bash
# Terminal 1: Start Kafka
docker-compose up -d
```

#### Create the Kafka topic

Run this command once:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
    --create --topic wiki-changes \
    --bootstrap-server localhost:9092 \
    --partitions 1 --replication-factor 1
```

#### Run the streaming pipeline

Open two separate terminals (with the Python environment activated).

**Terminal 2: Wikimedia -> Kafka**

```bash
python -m src.streaming_producer
```

**Terminal 3: Kafka -> Parquet**

```bash
python -m src.streaming_pipeline
```

Both processes must run for at least 15 to 30 minutes to capture a representative set of events.

Then, open `notebooks/04_streaming.ipynb` and run the cells to inspect the output stored under:

```text
data/streaming/output/wiki_changes/
```

#### Stop the pipeline

- Press `Ctrl+C` in terminals 2 and 3.
- Stop the Kafka container:

```bash
docker-compose down
```

## Data and Interfaces

### IMDb Data Tables

| Table | Key | Links |
|---|---|---|
| `title.basics` | `tconst` | Title and genre dimensions |
| `name.basics` | `nconst` | `knownForTitles[]` -> `title.basics.tconst` |
| `title.akas` | `titleId`, `ordering` | `titleId` -> `title.basics.tconst` |
| `title.crew` | `tconst` | `tconst` → `title.basics`, `directors[]` / `writers[]` -> `name.basics.nconst` |
| `title.episode` | `tconst` | Episode and series via `tconst` / `parentTconst` → `title.basics.tconst` |
| `title.principals` | `tconst`, `ordering` | `tconst` -> `title.basics`, `nconst` -> `name.basics` |
| `title.ratings` | `tconst` | `tconst` -> `title.basics` |

### Prepared Data

`data/processed/LATEST` points to the last completed preparation.

Its subfolder contains:

- `tables/<table>`
- `quality/missing`
- `quality/anomalies` (Parquet)
- `manifest.json`

Other parts can call `prepared_snapshot` and `read_prepared` in `src/imdb_loader.py` from their own notebooks.

An interrupted run leaves the pointer in place. Old preparation directories can be removed locally once unused.

### Configuration

`IMDB_DATA_DIR` redirects the data tree. A relative path is resolved from the repository.

The following environment variables configure the computation:

| Variable | Default | Description |
|---|---|---|
| `IMDB_SPARK_THREADS` | 4 | Number of Spark threads |
| `IMDB_SPARK_PARTITIONS` | 32 | Number of Spark partitions |
| `IMDB_SPARK_MEMORY` | 4g | Spark memory allocation |
| `IMDB_SCRATCH_DIR` | Existing temporary directory | Temporary directory used to decompress one TSV at a time |
| `IMDB_EXPAND_GZIP` | 1 | Set to `0` to read archives directly with Spark |

`IMDB_SCRATCH_DIR` designates an existing temporary directory to decompress one TSV at a time, without global loading in Python.

Set `IMDB_EXPAND_GZIP=0` to read archives directly with Spark if the temporary disk is limited. Each gzip archive is then read by a single Spark task.

### Data Quality and Relationships

Orphan references are counted after key cleaning.

Values are kept in the tables, and analytical joins on an entity require a parent.

List fields remain comma-separated, conforming to the input format.

## Operating the Results

The thresholds for the eight questions are grouped in `PARAMETERS` at the beginning of the second notebook.

Each execution exports its DataFrames to:

```text
data/results/<preparation>/<execution>/
```

`summary.json` records the thresholds, volumes, and language coverage.

The presence of this file indicates a completed execution.

Tables can be read back with:

```python
spark.read.parquet(path)
```

Public credits are limited to principals for actors and several other professions.

Ratings and votes are from the snapshot used, including for older films.

The notebook displays the assumptions of each query as well as the tables intended for verification.

Preparation exports contain profiles, anomalies, and examples.

Exploration exports include detail by genre, annual ranking, comparisons, and individual results.

## Original Language of Films

IMDb provides the language of an alternative title in `title.akas`.

In the absence of an external reference, Q6 exports only an indicator named `title_proxy` and its coverage.

To compute Q6 on the original languages of films, create the UTF-8 file:

```text
data/reference/original_languages.csv
```

With the following header:

```csv
tconst,language,source_uri
```

Provide:

- An IMDb title identifier.
- A normalized ISO 639-1 or ISO 639-3 language code.
- A URL justifying the language.

Use one line per title-language pair and the same code for the same language.

The verified mode analyzes the coverage of the provided file and records its SHA-256 fingerprint.

Results remain bounded to documented films and public credits.

Wikidata offers a lead to build this reference by joining IMDb identifiers to original language declarations, after verifying the sources.

- IMDb identifier: [P345](https://www.wikidata.org/wiki/Property:P345)
- Original language: [P364](https://www.wikidata.org/wiki/Property:P364)

## Data Enrichment

The enrichment step (`03_data_enrichment.ipynb`) fetches film synopses and biographical information from Wikidata and Wikipedia.

### Wikimedia Contact Requirements

**Important:** Wikimedia requires an explicit contact email in the `User-Agent` header.

The `CONTACT` variable is set in the notebook configuration cell.

It can also be provided via the `WIKI_CONTACT` environment variable.

The pipeline respects API rate limits (0.2s interval) and caches every answer so that an interrupted run resumes without asking again.

## AI Usage

OpenAI Codex and Claude were used for developing:

- The loading logic.
- Quality checks.
- Data cleaning.
- Exploration queries.
- The notebooks.
- Tests.
- This technical guide.

Each member writes their own explanations and reasoning in `REPORT.md`.
