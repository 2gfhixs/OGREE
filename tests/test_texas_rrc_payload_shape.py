"""Phase 9 — Texas RRC adapter + chain scoring tests."""
from __future__ import annotations

import os

import pytest

from ogree_alpha.adapters.texas_rrc import (
    _canonicalize_payload,
    _clean_str,
    _normalize_api,
    _normalize_lineage_id,
    _normalize_type,
    _parse_dt,
    iter_fixture_events,
)


# --- Normalization unit tests (no DB required) ---

def test_clean_str_strips_and_collapses():
    assert _clean_str("  Ring  Energy,  Inc.  ") == "Ring Energy, Inc."
    assert _clean_str("") is None
    assert _clean_str(None) is None
    assert _clean_str("   ") is None


def test_normalize_api():
    assert _normalize_api("42-301-00001") == "42-301-00001"
    assert _normalize_api("  42-301-00001  ") == "42-301-00001"
    assert _normalize_api("") is None
    assert _normalize_api(None) is None


def test_normalize_type_aliases():
    assert _normalize_type("permit_filed") == "permit_filed"
    assert _normalize_type("drilling_permit") == "drilling_permit"
    assert _normalize_type("spud") == "spud_reported"
    assert _normalize_type("  Drill_Result  ") == "drill_result"
    assert _normalize_type("production") == "production_reported"
    assert _normalize_type("p_and_a") == "plugging_report"
    assert _normalize_type(None) == "unknown"
    assert _normalize_type("") == "unknown"
    # Unrecognized type passes through lowered
    assert _normalize_type("something_new") == "something_new"


def test_normalize_lineage_id_prefers_api():
    assert _normalize_lineage_id({"api": "42-301-00001", "permit_no": "900001"}) == "TX:42-301-00001"


def test_normalize_lineage_id_falls_back_to_permit():
    assert _normalize_lineage_id({"permit_no": "900001"}) == "TX:permit:900001"


def test_normalize_lineage_id_none_when_empty():
    assert _normalize_lineage_id({}) is None
    assert _normalize_lineage_id({"api": "", "permit_no": ""}) is None


def test_canonicalize_payload_sets_region():
    p = _canonicalize_payload({"type": "permit_filed"})
    assert p["region"] == "Texas"

    p2 = _canonicalize_payload({"type": "permit_filed", "region": ""})
    assert p2["region"] == "Texas"


def test_canonicalize_payload_cleans_operator():
    p = _canonicalize_payload({"type": "permit_filed", "operator": "  Ring  Energy,  Inc.  "})
    assert p["operator"] == "Ring Energy, Inc."

    p2 = _canonicalize_payload({"type": "permit_filed", "operator": ""})
    assert p2["operator"] is None


def test_canonicalize_payload_carries_permit_id():
    p = _canonicalize_payload({"type": "permit_filed", "permit_no": "900001"})
    assert p["permit_id"] == "900001"
    assert p["permit_no"] == "900001"


def test_canonicalize_payload_numeric_fields():
    p = _canonicalize_payload({"type": "completion_reported", "ip_boed": 850, "depth_proposed": "10500"})
    assert p["ip_boed"] == 850.0
    assert p["depth_proposed"] == 10500.0

    # Bad numeric
    p2 = _canonicalize_payload({"type": "completion_reported", "ip_boed": "N/A"})
    assert p2["ip_boed"] is None


def test_parse_dt_formats():
    # ISO with Z
    dt = _parse_dt("2026-01-15T08:00:00Z")
    assert dt is not None
    assert dt.year == 2026

    # RRC-style date
    dt2 = _parse_dt("01/15/2026")
    assert dt2 is not None
    assert dt2.month == 1 and dt2.day == 15

    # None / empty
    assert _parse_dt(None) is None
    assert _parse_dt("") is None


# --- Fixture loading tests (no DB required) ---

def test_fixture_loads_all_events():
    events = list(iter_fixture_events())
    assert len(events) >= 14  # our expanded fixture


def test_fixture_all_events_have_payload():
    for e in iter_fixture_events():
        payload = e.get("payload_json")
        assert isinstance(payload, dict), f"Bad payload in event {e.get('source_event_id')}"
        assert "type" in payload


def test_fixture_lineage_derivable_for_all():
    for e in iter_fixture_events():
        payload = _canonicalize_payload(e.get("payload_json") or {})
        lineage = _normalize_lineage_id(payload)
        assert lineage is not None and len(lineage) > 0, (
            f"No lineage for event {e.get('source_event_id')}: {payload}"
        )


def test_fixture_covers_event_lifecycle():
    """Fixture should contain at least permit, spud, drill, completion, production."""
    types_seen = set()
    for e in iter_fixture_events():
        payload = e.get("payload_json") or {}
        t = _normalize_type(payload.get("type"))
        types_seen.add(t)

    expected = {"permit_filed", "spud_reported", "drill_result", "completion_reported", "production_reported"}
    missing = expected - types_seen
    assert not missing, f"Fixture missing event types: {missing}"


def test_fixture_multiple_operators():
    """Fixture should have more than one operator."""
    operators = set()
    for e in iter_fixture_events():
        payload = _canonicalize_payload(e.get("payload_json") or {})
        op = payload.get("operator")
        if op:
            operators.add(op)
    assert len(operators) >= 3, f"Only found operators: {operators}"


def test_fixture_handles_dirty_data():
    """Fixture includes edge cases: empty operator, empty region, duplicate API."""
    events = list(iter_fixture_events())
    source_ids = [e.get("source_event_id") for e in events]
    # Our dirty-data events
    assert "txp_040" in source_ids  # empty operator + empty region
    assert "txp_041" in source_ids  # missing region key entirely


# --- DB-dependent tests ---

@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_tx_rrc_fixture_ingest_counts():
    from ogree_alpha.adapters.texas_rrc import ingest_fixture_to_db

    inserted, processed = ingest_fixture_to_db()
    assert processed >= 14
    # Second run should insert 0 (idempotent)
    inserted2, processed2 = ingest_fixture_to_db()
    assert processed2 == processed
    assert inserted2 == 0


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_tx_chain_scoring_with_fixture():
    """After ingesting TX fixture, chain scoring should find wells with full progression."""
    from ogree_alpha.adapters.texas_rrc import ingest_fixture_to_db
    from ogree_alpha.chain_view import compute_chain_scores, load_recent_events

    ingest_fixture_to_db()
    events = load_recent_events(hours=9999)  # get all
    rows = compute_chain_scores(events)

    # Find the Permian well (42-301-00001) — should have full chain
    permian = [r for r in rows if r["lineage_id"] == "TX:42-301-00001"]
    assert len(permian) == 1
    p = permian[0]
    assert p["has_permit"] is True
    assert p["has_well"] is True
    assert p["score"] >= 0.8  # permit + spud + well + production = 1.0
