from __future__ import annotations

import os

import pytest

from ogree_alpha.adapters.federal_register_rules import (
    _canonicalize_payload,
    _derive_lineage_id,
    _normalize_type,
    ingest_fixture_to_db,
    iter_fixture_events,
)


def test_normalize_type_aliases():
    assert _normalize_type("final_rule") == "policy_final_rule"
    assert _normalize_type("rule_published") == "policy_final_rule"
    assert _normalize_type("policy_final_rule") == "policy_final_rule"


def test_fixture_loads_and_has_expected_type():
    events = list(iter_fixture_events())
    assert len(events) >= 3
    types_seen = {(e.get("payload_json") or {}).get("type") for e in events}
    assert {"final_rule", "rule_published", "policy_final_rule"} & types_seen


def test_canonicalize_resolves_lineage():
    payload = _canonicalize_payload(
        {
            "type": "final_rule",
            "company": "Ur-Energy Inc.",
            "document_number": "2026-02911",
        }
    )
    assert payload["type"] == "policy_final_rule"
    assert payload["company_id"] == "UR_ENERGY"
    assert payload["lineage_id"] == "POLICY:UR_ENERGY"
    assert _derive_lineage_id(payload) == "POLICY:UR_ENERGY"


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_federal_register_ingest_idempotent():
    inserted, processed = ingest_fixture_to_db()
    assert processed >= 3
    inserted2, _ = ingest_fixture_to_db()
    assert inserted2 == 0
