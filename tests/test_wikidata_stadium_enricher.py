import pytest

from scrapers import wikidata_stadium_enricher as enricher


def _entity_claim(qid: str, *, rank: str = "normal", ended: bool = False) -> dict:
    claim = {
        "rank": rank,
        "mainsnak": {
            "datavalue": {
                "value": {"id": qid},
            },
        },
    }
    if ended:
        claim["qualifiers"] = {enricher._END_TIME_QUALIFIER: [{}]}
    return claim


@pytest.mark.unit
def test_best_claim_prefers_current_non_deprecated_value():
    claims = {
        "P115": [
            _entity_claim("QOLD", rank="preferred", ended=True),
            _entity_claim("QBAD", rank="deprecated"),
            _entity_claim("QCURRENT", rank="normal"),
        ],
    }

    assert enricher._best_claim(claims, "P115") == "QCURRENT"


@pytest.mark.unit
def test_row_from_entity_uses_current_image_claim():
    entity = {
        "claims": {
            "P18": [
                {
                    "rank": "preferred",
                    "qualifiers": {enricher._END_TIME_QUALIFIER: [{}]},
                    "mainsnak": {"datavalue": {"value": "old-stadium.jpg"}},
                },
                {
                    "rank": "normal",
                    "mainsnak": {"datavalue": {"value": "current-stadium.jpg"}},
                },
            ],
        },
        "labels": {"en": {"value": "Example Stadium"}},
        "sitelinks": {},
    }

    row = enricher._row_from_entity("Q1", entity)

    assert row["image_url"].endswith("current-stadium.jpg")


@pytest.mark.unit
def test_query_by_club_uses_current_home_venue(monkeypatch):
    club_entity = {
        "claims": {
            "P115": [
                _entity_claim("QOLD", rank="preferred", ended=True),
                _entity_claim("QCURRENT", rank="normal"),
            ],
        },
    }
    current_venue = {
        "claims": {
            "P625": [
                {
                    "mainsnak": {
                        "datavalue": {
                            "value": {"latitude": 40.1, "longitude": -3.7},
                        },
                    },
                },
            ],
        },
        "labels": {"en": {"value": "Current Stadium"}},
        "sitelinks": {},
    }

    monkeypatch.setattr(enricher, "_search_entity_id", lambda *args, **kwargs: "QCLUB")
    monkeypatch.setattr(
        enricher,
        "_fetch_entity",
        lambda qid: club_entity if qid == "QCLUB" else current_venue,
    )
    monkeypatch.setattr(enricher, "_fetch_entities", lambda qids: {"QCURRENT": current_venue})

    row = enricher.query_wikidata_by_club("Example FC")

    assert row["wikidata_qid"] == "QCURRENT"
    assert row["latitude"] == 40.1


@pytest.mark.unit
def test_enrich_stadium_bypasses_cached_coords_when_image_required(monkeypatch):
    cache = {
        "v3|example stadium|example fc|": {
            "fetched_at": "2099-01-01T00:00:00+00:00",
            "data": {"latitude": 40.1, "longitude": -3.7},
        },
    }

    monkeypatch.setattr(
        enricher,
        "resolve_stadium_coords_with_fallback",
        lambda *args, **kwargs: {"latitude": 40.1, "longitude": -3.7, "wikidata_qid": "Q1"},
    )
    monkeypatch.setattr(enricher, "_derive_timezone", lambda lat, lon: "Europe/Madrid")
    monkeypatch.setattr(enricher, "_derive_altitude", lambda lat, lon: 600)
    monkeypatch.setattr(
        enricher,
        "resolve_stadium_image",
        lambda **kwargs: {
            "wikidata_qid": "Q1",
            "image_url": "https://commons.wikimedia.org/wiki/Special:FilePath/example.jpg",
            "wikipedia_url": "https://en.wikipedia.org/wiki/Example_Stadium",
        },
    )

    row = enricher.enrich_stadium(
        {"stadium_name": "Example Stadium", "team": "Example FC"},
        cache=cache,
        require_image=True,
    )

    assert row["image_url"].endswith("example.jpg")
    assert row["wikidata_qid"] == "Q1"


@pytest.mark.unit
def test_enrich_all_selects_rows_missing_only_image(monkeypatch):
    captured = {}

    class FakeResult:
        def mappings(self):
            return self

        def fetchall(self):
            return []

    class FakeConn:
        def execute(self, sql, params=None):
            captured["sql"] = str(sql)
            return FakeResult()

    monkeypatch.setattr(enricher, "_load_cache", lambda: {})
    monkeypatch.setattr(enricher, "_save_cache", lambda cache: None)

    enricher._enrich_with_connection(FakeConn(), "SELECT 1", {}, dry_run=True, limit=None)
    enricher.enrich_all_stadiums(FakeConn(), dry_run=True)

    assert "s.image_url IS NULL" in captured["sql"]
