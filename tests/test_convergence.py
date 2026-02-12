"""Convergence scoring tests (Task 2)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ogree_alpha.alert_generator import build_alert
from ogree_alpha.chain_view import compute_chain_scores
from ogree_alpha.convergence import apply_convergence


def _evt(
    event_time: datetime,
    *,
    lineage_id: str,
    event_type: str,
    company: str | None = None,
    company_id: str | None = None,
    operator: str | None = None,
    filer_name: str | None = None,
) -> dict:
    payload = {
        "lineage_id": lineage_id,
        "type": event_type,
    }
    if company is not None:
        payload["company"] = company
    if company_id is not None:
        payload["company_id"] = company_id
    if operator is not None:
        payload["operator"] = operator
    if filer_name is not None:
        payload["filer_name"] = filer_name
    return {"payload_json": payload, "event_time": event_time, "ingest_time": event_time}


def test_apply_convergence_respects_window():
    anchor = datetime(2026, 2, 10, 0, 0, tzinfo=timezone.utc)
    rows = [{"lineage_id": "L1", "last_event_time": anchor}]
    events = [
        _evt(anchor - timedelta(days=40), lineage_id="L1", event_type="permit_filed"),
        _evt(anchor - timedelta(days=10), lineage_id="L1", event_type="insider_buy", filer_name="Insider A"),
    ]
    enriched = apply_convergence(rows, events, window_days=30)
    assert enriched[0]["convergence_score"] == 1
    assert enriched[0]["convergence_categories"] == ["E"]


def test_chain_scores_include_company_level_cross_source_convergence():
    t0 = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    company = "Permian Basin Resources Inc"
    events = [
        # TX lineage carries A + B
        _evt(t0, lineage_id="TX:42-301-00001", event_type="permit_filed", operator=company),
        _evt(t0 + timedelta(days=1), lineage_id="TX:42-301-00001", event_type="drill_result", operator=company),
        # Cross-source company signals carry D + E + F
        _evt(
            t0 + timedelta(days=2),
            lineage_id="SEC:PERMIAN_BASIN_RES",
            event_type="insider_buy",
            company=company,
            company_id="PERMIAN_BASIN_RES",
            filer_name="Dana Morgan",
        ),
        _evt(
            t0 + timedelta(days=3),
            lineage_id="MD:financing",
            event_type="financing_closed",
            company=company,
            company_id="PERMIAN_BASIN_RES",
        ),
        _evt(
            t0 + timedelta(days=4),
            lineage_id="MD:policy",
            event_type="policy_designation",
            company=company,
        ),
    ]

    rows = compute_chain_scores(events, convergence_window_days=30)
    tx_rows = [r for r in rows if r["lineage_id"] == "TX:42-301-00001"]
    assert len(tx_rows) == 1
    tx = tx_rows[0]
    assert tx["convergence_score"] >= 5
    assert set(tx["convergence_categories"]) >= {"A", "B", "D", "E", "F"}


def test_alert_summary_mentions_convergence_when_score_is_high():
    row = {
        "lineage_id": "L_CONV",
        "score": 1.0,
        "has_permit": True,
        "has_well": True,
        "operator": "Operator",
        "region": "AK",
        "permit_id": "P1",
        "last_event_time": datetime(2026, 2, 10, 0, 0, tzinfo=timezone.utc),
        "convergence_score": 3,
        "convergence_categories": ["A", "B", "E"],
    }
    alert = build_alert(row, utc_date="2026-02-10")
    assert "convergence=3" in alert["summary"]


def test_alert_summary_omits_convergence_when_below_threshold():
    row = {
        "lineage_id": "L_NON_CONV",
        "score": 0.6,
        "has_permit": True,
        "has_well": True,
        "operator": "Operator",
        "region": "AK",
        "permit_id": "P2",
        "last_event_time": datetime(2026, 2, 10, 0, 0, tzinfo=timezone.utc),
        "convergence_score": 2,
        "convergence_categories": ["A", "B"],
    }
    alert = build_alert(row, utc_date="2026-02-10")
    assert "convergence=" not in alert["summary"]
