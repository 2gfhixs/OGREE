"""Phase 10 — REE/Uranium adapter + chain scoring tests."""
from __future__ import annotations

import os

import pytest

from ogree_alpha.adapters.ree_uranium import (
    _canonicalize_payload,
    _clean_str,
    _derive_lineage_id,
    _normalize_commodity,
    _normalize_type,
    _parse_dt,
    iter_fixture_events,
)


# --- Normalization unit tests (no DB required) ---

def test_normalize_commodity():
    assert _normalize_commodity("REE") == "REE"
    assert _normalize_commodity("rare earths") == "REE"
    assert _normalize_commodity("uranium") == "uranium"
    assert _normalize_commodity("U3O8") == "uranium"
    assert _normalize_commodity("u") == "uranium"
    assert _normalize_commodity(None) is None
    assert _normalize_commodity("") is None


def test_normalize_type():
    assert _normalize_type("drill_assay") == "drill_assay"
    assert _normalize_type("pea_published") == "pea_published"
    assert _normalize_type("  Financing_Closed  ") == "financing_closed"
    assert _normalize_type(None) == "unknown"


def test_derive_lineage_id_project_based():
    payload = {"company": "Frontier Rare Earths Ltd", "project": "Red Mountain"}
    lid = _derive_lineage_id(payload)
    assert lid is not None and len(lid) == 20

    # Same company+project = same lineage
    lid2 = _derive_lineage_id(payload)
    assert lid == lid2

    # Different project = different lineage
    payload2 = {"company": "Frontier Rare Earths Ltd", "project": "Other Project"}
    lid3 = _derive_lineage_id(payload2)
    assert lid3 != lid


def test_derive_lineage_id_policy_event():
    payload = {"type": "policy_designation", "policy": "DOE Critical Materials", "commodity": "REE"}
    lid = _derive_lineage_id(payload)
    assert lid is not None


def test_derive_lineage_id_none_when_empty():
    assert _derive_lineage_id({}) is None
    assert _derive_lineage_id({"company": "Foo"}) is None  # no project


def test_canonicalize_payload_normalizes_tickers():
    p = _canonicalize_payload({"type": "drill_assay", "tickers": ["FRE.V", "FREFF"]})
    assert p["tickers"] == ["FRE.V", "FREFF"]

    p2 = _canonicalize_payload({"type": "drill_assay", "tickers": "FRE.V, FREFF"})
    assert p2["tickers"] == ["FRE.V", "FREFF"]

    p3 = _canonicalize_payload({"type": "drill_assay"})
    assert p3["tickers"] == []


def test_canonicalize_payload_numeric_fields():
    p = _canonicalize_payload({
        "type": "drill_assay",
        "treo_pct": 3.82,
        "interval_m": "35.8",
        "u3o8_ppm": 2850,
    })
    assert p["treo_pct"] == 3.82
    assert p["interval_m"] == 35.8
    assert p["u3o8_ppm"] == 2850.0


def test_parse_dt_formats():
    dt = _parse_dt("2026-01-15T12:00:00Z")
    assert dt is not None and dt.year == 2026

    dt2 = _parse_dt("2026-01-15")
    assert dt2 is not None

    assert _parse_dt(None) is None


# --- Fixture loading tests ---

def test_fixture_loads_all_events():
    events = list(iter_fixture_events())
    assert len(events) >= 15


def test_fixture_all_events_have_payload():
    for e in iter_fixture_events():
        payload = e.get("payload_json")
        assert isinstance(payload, dict)
        assert "type" in payload


def test_fixture_covers_ree_lifecycle():
    types_seen = set()
    for e in iter_fixture_events():
        t = (e.get("payload_json") or {}).get("type")
        if t:
            types_seen.add(t)
    expected = {"claims_staked", "exploration_permit", "drill_assay",
                "resource_estimate", "financing_closed"}
    missing = expected - types_seen
    assert not missing, f"Missing types: {missing}"


def test_fixture_covers_uranium_lifecycle():
    types_seen = set()
    for e in iter_fixture_events():
        pj = e.get("payload_json") or {}
        if (pj.get("commodity") or "").lower() == "uranium":
            t = pj.get("type")
            if t:
                types_seen.add(t)
    expected = {"claims_staked", "drill_assay", "resource_estimate", "offtake_agreement"}
    missing = expected - types_seen
    assert not missing, f"Missing uranium types: {missing}"


def test_fixture_has_policy_event():
    types = [e.get("payload_json", {}).get("type") for e in iter_fixture_events()]
    assert "policy_designation" in types


def test_fixture_has_pea():
    types = [e.get("payload_json", {}).get("type") for e in iter_fixture_events()]
    assert "pea_published" in types


def test_fixture_lineage_derivable_for_all():
    for e in iter_fixture_events():
        payload = _canonicalize_payload(e.get("payload_json") or {})
        lineage = _derive_lineage_id(payload)
        assert lineage is not None, f"No lineage for {e.get('source_event_id')}: {payload}"


def test_fixture_multiple_companies():
    companies = set()
    for e in iter_fixture_events():
        c = _clean_str((e.get("payload_json") or {}).get("company"))
        if c:
            companies.add(c)
    assert len(companies) >= 3, f"Only found: {companies}"


# --- DB-dependent tests ---

@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_ree_uranium_fixture_ingest_counts():
    from ogree_alpha.adapters.ree_uranium import ingest_fixture_to_db

    inserted, processed = ingest_fixture_to_db()
    assert processed >= 15
    # Idempotent
    inserted2, _ = ingest_fixture_to_db()
    assert inserted2 == 0


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_ree_chain_scoring():
    from ogree_alpha.adapters.ree_uranium import ingest_fixture_to_db
    from ogree_alpha.chain_view import compute_chain_scores, load_recent_events

    ingest_fixture_to_db()
    events = load_recent_events(hours=9999)
    rows = compute_chain_scores(events)

    # Frontier REE should have claims + drill + resource + deal = high score
    frontier_rows = [r for r in rows if r.get("company") == "Frontier Rare Earths Ltd"]
    assert len(frontier_rows) == 1
    fr = frontier_rows[0]
    assert fr["has_claims"] is True
    assert fr["has_drill_assay"] is True
    assert fr["has_resource"] is True
    assert fr["has_deal"] is True
    assert fr["score"] >= 0.8
