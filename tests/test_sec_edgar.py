"""SEC EDGAR adapter + insider signal scoring tests."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from ogree_alpha.adapters.sec_edgar import (
    _canonicalize_payload,
    _derive_lineage_id,
    _normalize_relationship,
    _normalize_type,
    _normalize_transaction_type,
    _parse_dt,
    iter_fixture_events,
)
from ogree_alpha.chain_view import compute_chain_scores


def test_normalize_type_aliases():
    assert _normalize_type("insider_buy") == "insider_buy"
    assert _normalize_type("purchase") == "insider_buy"
    assert _normalize_type("sell") == "insider_sell"
    assert _normalize_type("13G") == "institutional_13g"
    assert _normalize_type("form_13f") == "institutional_13f"


def test_normalize_relationship():
    assert _normalize_relationship("Chief Financial Officer") == "officer"
    assert _normalize_relationship("Director") == "director"
    assert _normalize_relationship("10 percent beneficial owner") == "10% owner"
    assert _normalize_relationship("Institutional Fund") == "institution"


def test_normalize_transaction_type_fallbacks():
    assert _normalize_transaction_type(None, normalized_event_type="insider_buy") == "purchase"
    assert _normalize_transaction_type(None, normalized_event_type="insider_sell") == "sale"
    assert _normalize_transaction_type(None, normalized_event_type="insider_option_exercise") == "exercise"
    assert _normalize_transaction_type("buy", normalized_event_type="institutional_13g") == "purchase"


def test_derive_lineage_prefers_company_id():
    payload = {"company_id": "PERMIAN_BASIN_RES", "company": "Permian Basin Resources Inc"}
    assert _derive_lineage_id(payload) == "SEC:PERMIAN_BASIN_RES"


def test_canonicalize_payload_resolves_company_and_computes_total():
    payload = _canonicalize_payload(
        {
            "type": "insider_buy",
            "filer_name": "Dana Morgan",
            "relationship": "Chief Financial Officer",
            "company": "Permian Basin Resources Inc",
            "tickers": "",
            "shares": 60000,
            "price_per_share": 2.1,
        }
    )
    assert payload["company_id"] == "PERMIAN_BASIN_RES"
    assert payload["lineage_id"] == "SEC:PERMIAN_BASIN_RES"
    assert payload["transaction_type"] == "purchase"
    assert payload["total_value"] == 126000.0
    assert payload["tickers"] == ["PBR"]


def test_parse_dt_formats():
    dt = _parse_dt("2026-02-10T15:20:00Z")
    assert dt is not None and dt.year == 2026
    dt2 = _parse_dt("2026-02-10")
    assert dt2 is not None
    assert _parse_dt(None) is None


def test_fixture_loads_and_covers_required_event_types():
    events = list(iter_fixture_events())
    assert len(events) >= 10
    types_seen = {(e.get("payload_json") or {}).get("type") for e in events}
    expected = {
        "insider_buy",
        "insider_sell",
        "insider_option_exercise",
        "institutional_13g",
        "institutional_13f",
    }
    missing = expected - types_seen
    assert not missing, f"Missing SEC fixture event types: {missing}"


def test_chain_scoring_adds_insider_buy_bonus_and_cluster_bonus():
    t0 = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "payload_json": {
                "lineage_id": "SEC:PERMIAN_BASIN_RES",
                "company": "Permian Basin Resources Inc",
                "type": "insider_buy",
                "filer_name": "Dana Morgan",
            },
            "event_time": t0,
            "ingest_time": t0,
        },
        {
            "payload_json": {
                "lineage_id": "SEC:PERMIAN_BASIN_RES",
                "company": "Permian Basin Resources Inc",
                "type": "insider_buy",
                "filer_name": "Ryan Cole",
            },
            "event_time": t0 + timedelta(days=14),
            "ingest_time": t0 + timedelta(days=14),
        },
    ]
    rows = compute_chain_scores(events)
    assert len(rows) == 1
    row = rows[0]
    assert row["has_insider_buy"] is True
    assert row["has_insider_buy_cluster"] is True
    assert row["score"] == 0.25


def test_chain_scoring_cluster_requires_distinct_insiders():
    t0 = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "payload_json": {
                "lineage_id": "SEC:EAGLE_FORD_ENERGY",
                "company": "Eagle Ford Energy LLC",
                "type": "insider_buy",
                "filer_name": "Sarah Patel",
            },
            "event_time": t0,
            "ingest_time": t0,
        },
        {
            "payload_json": {
                "lineage_id": "SEC:EAGLE_FORD_ENERGY",
                "company": "Eagle Ford Energy LLC",
                "type": "insider_buy",
                "filer_name": "Sarah Patel",
            },
            "event_time": t0 + timedelta(days=10),
            "ingest_time": t0 + timedelta(days=10),
        },
    ]
    rows = compute_chain_scores(events)
    assert rows[0]["has_insider_buy"] is True
    assert rows[0]["has_insider_buy_cluster"] is False
    assert rows[0]["score"] == 0.15


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_sec_fixture_ingest_counts():
    from ogree_alpha.adapters.sec_edgar import ingest_fixture_to_db

    inserted, processed = ingest_fixture_to_db()
    assert processed >= 10
    inserted2, _ = ingest_fixture_to_db()
    assert inserted2 == 0
