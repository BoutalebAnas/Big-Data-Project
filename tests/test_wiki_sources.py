"""Offline tests for the Wikidata / Wikipedia gatherer (no network, no Spark)."""

import json
import re

import pytest

from src import wiki_sources as ws

MATRIX = "https://en.wikipedia.org/wiki/The_Matrix"
KEANU = "https://en.wikipedia.org/wiki/Keanu_Reeves"


class FakeResponse:
    def __init__(self, payload, status=200, headers=None):
        self.payload, self.status_code, self.headers = payload, status, headers or {}

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Plays back prepared responses and records the parameters of each call."""

    def __init__(self, *responses):
        self.headers, self.responses, self.calls = {}, list(responses), []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(params)
        return self.responses.pop(0)


class FakeClient:
    """Answers like Wikidata and Wikipedia for two known identifiers."""

    def __init__(self):
        self.calls = []

    def get_json(self, url, params, headers=None):
        self.calls.append((url, params))
        if url == ws.WIKIDATA_SPARQL:
            known = {
                "tt0133093": {"imdb": {"value": "tt0133093"},
                              "item": {"value": "http://www.wikidata.org/entity/Q83495"},
                              "article": {"value": MATRIX}, "iso1": {"value": "en"}},
                "nm0000206": {"imdb": {"value": "nm0000206"},
                              "item": {"value": "http://www.wikidata.org/entity/Q43416"},
                              "article": {"value": KEANU}},
            }
            asked = re.findall(r'"((?:tt|nm)[0-9]+)"', params["query"])
            return {"results": {"bindings": [known[i] for i in asked if i in known]}}
        titles = params["titles"].split("|")
        return {"query": {"pages": [
            {"title": title, "fullurl": f"https://en.wikipedia.org/wiki/{title}",
             "lastrevid": 1, "extract": f"About {title}."}
            for title in titles
        ]}}


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(ws.time, "sleep", lambda seconds: None)


def write_ids(folder, *ids):
    folder.mkdir(parents=True)
    (folder / "part-00000.csv").write_text("imdb_id\n" + "\n".join(ids) + "\n", encoding="utf-8")


def lines(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_read_ids_streams_every_part_file_and_skips_blanks(tmp_path):
    write_ids(tmp_path / "ids", "tt1", "", "tt2")
    (tmp_path / "ids" / "part-00001.csv").write_text("imdb_id\nnm3\n", encoding="utf-8")
    (tmp_path / "ids" / "_SUCCESS").write_text("", encoding="utf-8")
    assert list(ws.read_ids(tmp_path / "ids", "imdb_id")) == ["tt1", "tt2", "nm3"]


def test_batched_and_article_title():
    assert list(ws.batched(range(5), 2)) == [[0, 1], [2, 3], [4]]
    assert ws.article_title("https://en.wikipedia.org/wiki/Am%C3%A9lie_(film)") == "Amélie (film)"


def test_wikidata_records_keep_explicit_misses():
    payload = {"results": {"bindings": [
        {"imdb": {"value": "tt1"}, "item": {"value": "Q1"}, "iso3": {"value": "fra"}},
    ]}}
    records = {r["imdb_id"]: r for r in ws.wikidata_records(payload, ["tt1", "tt2"], "now")}
    assert records["tt1"]["wikidata_uri"] == "Q1" and records["tt1"]["language_iso3"] == "fra"
    assert records["tt1"]["article_url"] is None
    assert records["tt2"]["wikidata_uri"] is None
    assert all(r["source"] == ws.WIKIDATA_SPARQL for r in records.values())


def test_page_records_follow_normalisation_and_redirects_and_flag_missing():
    payload = {"query": {
        "normalized": [{"from": "keanu Reeves", "to": "Keanu Reeves"}],
        "redirects": [{"from": "Keanu Reeves", "to": "Keanu Charles Reeves"}],
        "pages": [
            {"title": "Keanu Charles Reeves", "fullurl": "u", "lastrevid": 7, "extract": "Bio"},
            {"title": "Gone", "missing": True},
        ],
    }}
    keanu, gone = ws.page_records(
        payload,
        ["https://en.wikipedia.org/wiki/keanu_Reeves", "https://en.wikipedia.org/wiki/Gone"],
        "now",
    )
    assert keanu["page_title"] == "Keanu Charles Reeves" and keanu["revision_id"] == 7
    assert not keanu["missing"] and keanu["text"] == "Bio"
    assert gone["missing"] and gone["text"] is None


def test_fetch_wikidata_skips_invalid_ids_and_resumes(tmp_path):
    write_ids(tmp_path / "ids", "tt0133093", "nm0000206", "tt9999999", "bad'id", "tt0133093")
    cache, client = tmp_path / "wikidata.jsonl", FakeClient()

    assert ws.fetch_wikidata(client, ws.read_ids(tmp_path / "ids", "imdb_id"), cache) == 3
    assert [r["imdb_id"] for r in lines(cache)] == ["tt0133093", "nm0000206", "tt9999999"]
    assert "bad'id" not in client.calls[0][1]["query"]

    assert ws.fetch_wikidata(client, ws.read_ids(tmp_path / "ids", "imdb_id"), cache) == 0
    assert len(client.calls) == 1


def test_fetch_wikidata_limit_and_torn_last_line(tmp_path):
    cache = tmp_path / "wikidata.jsonl"
    assert ws.fetch_wikidata(FakeClient(), ["tt0133093", "nm0000206"], cache, limit=1) == 1
    with cache.open("a", encoding="utf-8") as stream:
        stream.write('{"imdb_id": "nm00')                  # crash in the middle of a line
    assert ws.fetch_wikidata(FakeClient(), ["tt0133093", "nm0000206"], cache) == 1
    records = []
    for line in cache.read_text(encoding="utf-8").splitlines():
        try:
            records.append(json.loads(line)["imdb_id"])
        except json.JSONDecodeError:
            continue
    assert records == ["tt0133093", "nm0000206"]


def test_fetch_articles_uses_full_text_for_films_and_batches_intros(tmp_path):
    cache, client = tmp_path / "wikidata.jsonl", FakeClient()
    ws.fetch_wikidata(client, ["tt0133093", "nm0000206"], cache)
    assert list(ws.article_urls(cache, "tt")) == [MATRIX]
    assert list(ws.article_urls(cache, "nm")) == [KEANU]

    ws.fetch_articles(client, ws.article_urls(cache, "tt"), tmp_path / "films.jsonl",
                      intro_only=False)
    film_params = client.calls[-1][1]
    assert "exintro" not in film_params and film_params["maxlag"] == "5"

    people = [f"https://en.wikipedia.org/wiki/P{i}" for i in range(25)]
    before = len(client.calls)
    ws.fetch_articles(client, people, tmp_path / "people.jsonl", intro_only=True)
    assert len(client.calls) - before == 2                  # 25 intros = 20 + 5
    assert client.calls[-1][1]["exintro"] == "1"
    assert lines(tmp_path / "films.jsonl")[0]["page_title"] == "The Matrix"
    assert len(lines(tmp_path / "people.jsonl")) == 25


def test_client_requires_contact_and_identifies_itself():
    with pytest.raises(ValueError):
        ws.PoliteClient("")
    client = ws.PoliteClient("team@example.org", session=FakeSession())
    assert "team@example.org" in client.session.headers["User-Agent"]


def test_client_retries_on_throttling_and_maxlag():
    session = FakeSession(
        FakeResponse({}, status=429, headers={"Retry-After": "1"}),
        FakeResponse({"error": {"code": "maxlag"}}),
        FakeResponse({"ok": True}),
    )
    client = ws.PoliteClient("team@example.org", min_interval=0, session=session)
    assert client.get_json("https://example.org", {}) == {"ok": True}
    assert len(session.calls) == 3


def test_client_raises_on_api_error_and_gives_up():
    session = FakeSession(FakeResponse({"error": {"code": "badvalue", "info": "x"}}))
    with pytest.raises(RuntimeError, match="API error"):
        ws.PoliteClient("team@example.org", min_interval=0, session=session).get_json("u", {})
    session = FakeSession(*[FakeResponse({}, status=503) for _ in range(2)])
    client = ws.PoliteClient("team@example.org", min_interval=0, max_retries=2, session=session)
    with pytest.raises(RuntimeError, match="Giving up"):
        client.get_json("u", {})
