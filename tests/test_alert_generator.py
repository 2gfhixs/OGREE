import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from ogree_alpha.alert_generator import build_alert, tier_for_score, generate_and_insert_alerts

def test_build_alert_propagates_company_id():
    """Phase 8.1: resolved company_id must appear in alert payload."""
    row = {
        "lineage_id": "L_PROP",
        "score": 1.0,
        "has_permit": True,
        "has_well": True,
        "operator": "Some Operator",
        "region": "AK",
        "permit_id": "P_PROP",
        "last_event_time": datetime(2026, 2, 7, 0, 0, tzinfo=timezone.utc),
    }
    # With explicit company_id
    alert_with = build_alert(row, utc_date="2026-02-07", company_id="RESOLVED_CO")
    assert alert_with["company_id"] == "RESOLVED_CO"

    # Without company_id (None)
    alert_without = build_alert(row, utc_date="2026-02-07", company_id=None)
    assert alert_without["company_id"] is None

    # IDs must still be stable regardless of company_id
    assert alert_with["alert_id"] == alert_without["alert_id"]

def test_tier_thresholds():
    assert tier_for_score(1.0) == "high"
    assert tier_for_score(0.6) == "medium"
    assert tier_for_score(0.4) == "low"
    assert tier_for_score(0.39) == ""


def test_build_alert_stable_id():
    row = {
        "lineage_id": "L3",
        "score": 1.0,
        "has_permit": True,
        "has_well": True,
        "operator": "Op3",
        "region": "AK",
        "permit_id": "P3",
        "last_event_time": datetime(2026, 2, 7, 0, 0, tzinfo=timezone.utc),
    }
    a1 = build_alert(row, utc_date="2026-02-07")
    a2 = build_alert(row, utc_date="2026-02-07")
    assert a1["alert_id"] == a2["alert_id"]
    assert len(a1["alert_id"]) == 24


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_generate_and_insert_alerts_idempotent(monkeypatch):
    # Use a unique lineage_id so we never collide with prior DB state.
    lid = f"LX_{uuid4().hex}"

    def _fake_load_recent_events(hours: int = 72):
        return [{"payload_json": {"type": "permit_filed", "lineage_id": lid, "operator": "Op", "region": "AK", "permit_id": "Px"}}]

    def _fake_compute_chain_scores(events):
        return [{
            "lineage_id": lid,
            "score": 1.0,
            "has_permit": True,
            "has_well": True,
            "operator": "Op",
            "region": "AK",
            "permit_id": "Px",
            "last_event_time": datetime(2026, 2, 7, 0, 0, tzinfo=timezone.utc),
        }]
    import ogree_alpha.alert_generator as ag
    monkeypatch.setattr(ag, "resolve_company", lambda operator=None, **kw: type("R", (), {"company_id": "COMPANY_X"})())
    monkeypatch.setattr(ag, "load_recent_events", _fake_load_recent_events)
    monkeypatch.setattr(ag, "compute_chain_scores", _fake_compute_chain_scores)

    n1 = generate_and_insert_alerts(hours=72, top_n=5)
    n2 = generate_and_insert_alerts(hours=72, top_n=5)
    assert n1 == 1
    assert n2 == 0
