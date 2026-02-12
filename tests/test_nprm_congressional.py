from __future__ import annotations

import os

import pytest

from ogree_alpha.adapters.nprm_congressional import (
    _canonicalize_payload,
    _derive_lineage_id,
    _normalize_type,
    ingest_fixture_to_db,
    iter_fixture_events,
)


def test_normalize_type_aliases():
    assert _normalize_type("nprm_open") == "policy_nprm_open"
    assert _normalize_type("comment_deadline") == "policy_comment_deadline"
    assert _normalize_type("congressional_trade") == "congressional_trade_disclosure"
    assert _normalize_type("committee_advance") == "legislation_committee_advance"


def test_fixture_covers_expected_event_types():
    events = list(iter_fixture_events())
    assert len(events) >= 4
    types_seen = {(e.get("payload_json") or {}).get("type") for e in events}
    expected = {
        "nprm_open",
        "comment_deadline",
        "congressional_trade_disclosure",
        "committee_advance",
        "policy_nprm_open",
    }
    assert expected & types_seen


def test_canonicalize_resolves_company_policy_lineage():
    payload = _canonicalize_payload(
        {
            "type": "committee_advance",
            "company": "Ur-Energy Inc.",
            "bill_id": "S.2411",
        }
    )
    assert payload["type"] == "legislation_committee_advance"
    assert payload["company_id"] == "UR_ENERGY"
    assert payload["lineage_id"] == "POLICY:UR_ENERGY"
    assert _derive_lineage_id(payload) == "POLICY:UR_ENERGY"


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_policy_ingest_idempotent():
    inserted, processed = ingest_fixture_to_db()
    assert processed >= 4
    inserted2, _ = ingest_fixture_to_db()
    assert inserted2 == 0
