from ogree_alpha import report_twice_daily as r


def test_report_no_alerts(monkeypatch):
    monkeypatch.setattr(r, "load_recent_alerts", lambda hours=12: [])
    out = r.render_report()
    assert "Top Alerts" in out["subject"]
    assert "No new alerts" in out["text"]
    assert "No new alerts" in out["html"]


def test_report_tier_sections(monkeypatch):
    monkeypatch.setattr(
        r,
        "load_recent_alerts",
        lambda hours=12: [
            {"tier": "high", "summary": "A", "event_time": "t1", "score_summary": {"score": 1.0}},
            {"tier": "medium", "summary": "B", "event_time": "t2", "score_summary": {"score": 0.6}},
            {"tier": "low", "summary": "C", "event_time": "t3", "score_summary": {"score": 0.4}},
        ],
    )
    out = r.render_report()
    assert "HIGH" in out["text"]
    assert "MEDIUM" in out["text"]
    assert "LOW" in out["text"]
    assert "<h3>HIGH</h3>" in out["html"]
    assert "A" in out["text"] and "B" in out["text"] and "C" in out["text"]
