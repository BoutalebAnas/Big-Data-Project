"""Polite gathering of human-authored data from Wikidata and Wikipedia.

This module only *collects* data. Responses are appended as JSON Lines with
the endpoint and retrieval time of every record; parsing, cleaning, schema
enforcement and joins all happen in Spark (see ``src/enrichment.py``).

Design choices that keep the gatherer generic and scalable:

* identifiers are streamed from the CSV folders written by Spark, so the same
  code works for 10 films or the full IMDb catalogue;
* every output file is an append-only cache keyed by identifier, so an
  interrupted run resumes where it stopped and never asks twice for a record;
* requests are serial, spaced by ``min_interval`` seconds, identified with a
  descriptive User-Agent, use ``maxlag`` on the MediaWiki API, and back off
  on HTTP 429/5xx using the server's ``Retry-After`` header.
"""

import csv
import itertools
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import requests

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_ARTICLE_PREFIX = "https://en.wikipedia.org/wiki/"
IMDB_ID = re.compile(r"^(tt|nm)[0-9]+$")
RETRY_STATUS = {429, 500, 502, 503, 504}

# P345 = IMDb ID, P364 = original language of film or TV show,
# P218 / P220 = ISO 639-1 / ISO 639-3 code of that language.
SPARQL_TEMPLATE = """
SELECT ?imdb ?item ?article ?iso1 ?iso3 WHERE {
  VALUES ?imdb { %s }
  ?item wdt:P345 ?imdb .
  OPTIONAL {
    ?article schema:about ?item ;
             schema:isPartOf <https://en.wikipedia.org/> .
  }
  OPTIONAL {
    ?item wdt:P364 ?language .
    OPTIONAL { ?language wdt:P218 ?iso1 . }
    OPTIONAL { ?language wdt:P220 ?iso3 . }
  }
}
"""


class PoliteClient:
    """Serial HTTP client that follows the Wikimedia API etiquette."""

    def __init__(self, contact, *, min_interval=0.5, max_retries=6, timeout=60, session=None):
        if not contact or "@" not in contact:
            raise ValueError(
                "Wikimedia requires a contact in the User-Agent: set WIKI_CONTACT to an e-mail."
            )
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = (
            f"ECE-BigData-IMDb-Enrichment/1.0 (student project; {contact}) "
            f"python-requests/{requests.__version__}"
        )
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.timeout = timeout
        self.requests_sent = 0
        self._last_request = 0.0

    def _throttle(self):
        wait = self.min_interval - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def get_json(self, url, params, headers=None):
        """Return the decoded JSON body, retrying politely on throttling or outages."""
        for attempt in range(self.max_retries):
            self._throttle()
            delay = 2 ** attempt
            try:
                response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
                self.requests_sent += 1
            except requests.RequestException:
                time.sleep(delay)
                continue
            if response.status_code in RETRY_STATUS:
                time.sleep(_retry_after(response, delay))
                continue
            response.raise_for_status()
            payload = response.json()
            error = payload.get("error") if isinstance(payload, dict) else None
            if error and error.get("code") == "maxlag":
                time.sleep(_retry_after(response, 5))
                continue
            if error:
                raise RuntimeError(f"API error from {url}: {error}")
            return payload
        raise RuntimeError(f"Giving up on {url} after {self.max_retries} attempts.")


def _retry_after(response, default):
    try:
        return min(float(response.headers.get("Retry-After", default)), 300)
    except ValueError:
        return default


def _now():
    return datetime.now(timezone.utc).isoformat()


def read_ids(folder, column):
    """Stream identifiers from the CSV part files written by Spark."""
    for path in sorted(Path(folder).glob("part-*.csv")):
        with path.open(newline="", encoding="utf-8") as stream:
            for row in csv.DictReader(stream):
                value = (row.get(column) or "").strip()
                if value:
                    yield value


def batched(values, size):
    iterator = iter(values)
    while batch := list(itertools.islice(iterator, size)):
        yield batch


def _completed(path, key):
    """Keys already present in a JSON Lines cache (a torn last line is ignored)."""
    done = set()
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                try:
                    done.add(json.loads(line)[key])
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
    return done


def _open_append(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size:
        with path.open("rb") as stream:
            stream.seek(-1, 2)
            torn = stream.read(1) != b"\n"
        if torn:
            with path.open("a", encoding="utf-8") as stream:
                stream.write("\n")
    return path.open("a", encoding="utf-8")


def article_title(url):
    """Wikipedia page title from an article URL returned by Wikidata."""
    return unquote(url[len(WIKIPEDIA_ARTICLE_PREFIX):]).replace("_", " ")


def wikidata_records(payload, batch, retrieved_at):
    """Turn one SPARQL answer into cache records, including explicit misses."""
    records, found = [], set()
    for binding in payload["results"]["bindings"]:
        imdb_id = binding["imdb"]["value"]
        found.add(imdb_id)
        records.append({
            "imdb_id": imdb_id,
            "wikidata_uri": binding["item"]["value"],
            "article_url": binding.get("article", {}).get("value"),
            "language_iso1": binding.get("iso1", {}).get("value"),
            "language_iso3": binding.get("iso3", {}).get("value"),
            "retrieved_at": retrieved_at,
            "source": WIKIDATA_SPARQL,
        })
    for imdb_id in batch:
        if imdb_id not in found:
            records.append({
                "imdb_id": imdb_id, "wikidata_uri": None, "article_url": None,
                "language_iso1": None, "language_iso3": None,
                "retrieved_at": retrieved_at, "source": WIKIDATA_SPARQL,
            })
    return records


def fetch_wikidata(client, ids, output, *, batch_size=150, limit=None, progress_every=20):
    """Map IMDb identifiers to Wikidata items, English articles and film languages."""
    output = Path(output)
    done = _completed(output, "imdb_id")
    pending = (i for i in dict.fromkeys(ids) if i not in done and IMDB_ID.match(i))
    if limit is not None:
        pending = itertools.islice(pending, limit)
    written = 0
    with _open_append(output) as sink:
        for number, batch in enumerate(batched(pending, batch_size), start=1):
            query = SPARQL_TEMPLATE % " ".join(f'"{imdb_id}"' for imdb_id in batch)
            payload = client.get_json(
                WIKIDATA_SPARQL, {"query": query, "format": "json"},
                headers={"Accept": "application/sparql-results+json"},
            )
            for record in wikidata_records(payload, batch, _now()):
                sink.write(json.dumps(record, ensure_ascii=False) + "\n")
            sink.flush()
            written += len(batch)
            if number % progress_every == 0:
                print(f"  Wikidata: {written:,} new identifiers looked up")
    return written


def article_urls(wikidata_cache, prefix):
    """Distinct English article URLs for identifiers starting with ``prefix``."""
    seen = set()
    with Path(wikidata_cache).open(encoding="utf-8") as stream:
        for line in stream:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = record.get("article_url")
            if url and record["imdb_id"].startswith(prefix) and url not in seen:
                seen.add(url)
                yield url


def page_records(payload, urls, retrieved_at):
    """Match MediaWiki pages back to the requested URLs through redirects."""
    query = payload.get("query", {})
    hops = {item["from"]: item["to"] for item in query.get("normalized", [])}
    hops.update({item["from"]: item["to"] for item in query.get("redirects", [])})
    pages = {page["title"]: page for page in query.get("pages", [])}
    records = []
    for url in urls:
        title = article_title(url)
        for _ in range(3):
            if title in pages or title not in hops:
                break
            title = hops[title]
        page = pages.get(title, {})
        missing = not page or page.get("missing", False) or page.get("invalid", False)
        records.append({
            "article_url": url,
            "page_title": page.get("title"),
            "page_url": page.get("fullurl"),
            "revision_id": page.get("lastrevid"),
            "text": None if missing else page.get("extract"),
            "missing": bool(missing),
            "retrieved_at": retrieved_at,
            "source": WIKIPEDIA_API,
        })
    return records


def fetch_articles(client, urls, output, *, intro_only, limit=None, progress_every=100):
    """Download plain-text Wikipedia extracts.

    Full articles (needed to find the plot section) can only be requested one
    page at a time; introductions (used for biographies) go 20 at a time.
    """
    output = Path(output)
    done = _completed(output, "article_url")
    pending = (url for url in urls if url not in done)
    if limit is not None:
        pending = itertools.islice(pending, limit)
    params = {
        "action": "query", "format": "json", "formatversion": "2", "redirects": "1",
        "prop": "extracts|info", "inprop": "url", "explaintext": "1",
        "exsectionformat": "wiki", "maxlag": "5",
    }
    if intro_only:
        params.update(exintro="1", exlimit="20")
    size = 20 if intro_only else 1
    written = 0
    with _open_append(output) as sink:
        for number, batch in enumerate(batched(pending, size), start=1):
            titles = "|".join(article_title(url) for url in batch)
            payload = client.get_json(WIKIPEDIA_API, {**params, "titles": titles})
            for record in page_records(payload, batch, _now()):
                sink.write(json.dumps(record, ensure_ascii=False) + "\n")
            sink.flush()
            written += len(batch)
            if number % progress_every == 0:
                print(f"  Wikipedia: {written:,} new articles fetched")
    return written
