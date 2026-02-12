from __future__ import annotations

from ogree_alpha.observability import (
    render_text,
    summarize_alert_rows,
    summarize_chain_rows,
)


def test_summarize_chain_rows_counts():
    rows = [
        {
            "score": 1.0,
            "has_insider_buy": True,
            "convergence_score": 3,
            "company_id": "PERMIAN_RESOURCES",
        },
        {
            "score": 0.55,
            "has_insider_buy": False,
            "convergence_score": 2,
            "company_id": None,
        },
    ]
    out = summarize_chain_rows(rows)
    assert out["lineages"] == 2
    assert out["lineages_high_score"] == 1
    assert out["lineages_with_insider_signal"] == 1
    assert out["lineages_convergence_watch"] == 1
    assert out["lineages_convergence_3plus"] == 1
    assert out["lineage_company_resolution_rate_pct"] == 50.0


def test_summarize_alert_rows_counts():
    alerts = [
        {
            "tier": "high",
            "company_id": "PERMIAN_RESOURCES",
            "score_summary": {"score": 1.0, "convergence_score": 3},
        },
        {
            "tier": "low",
            "company_id": None,
            "score_summary": {"score": 0.35, "convergence_score": 0},
        },
        {
            "tier": "medium",
            "company_id": "UCORE_RARE_METALS",
            "score_summary": {"score": 0.6, "convergence_score": 2},
        },
    ]
    out = summarize_alert_rows(alerts)
    assert out["alerts"] == 3
    assert out["tier_counts"]["high"] == 1
    assert out["tier_counts"]["medium"] == 1
    assert out["tier_counts"]["low"] == 1
    assert out["alerts_with_company_id"] == 2
    assert out["alerts_convergence_3plus"] == 1
    assert out["alert_company_resolution_rate_pct"] == 66.67


def test_render_text_contains_sections():
    snapshot = {
        "generated_at": "2026-02-12T00:00:00Z",
        "event_window_hours": 72,
        "alert_window_hours": 24,
        "source_counts": {"ree_uranium": 10, "sec_edgar": 5},
        "chain": summarize_chain_rows([]),
        "alerts": summarize_alert_rows([]),
    }
    text = render_text(snapshot)
    assert "Source counts:" in text
    assert "Chain health:" in text
    assert "Alert health:" in text
    assert "ree_uranium" in text
