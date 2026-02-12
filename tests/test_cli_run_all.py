from __future__ import annotations

import pytest
import typer

from ogree_alpha import cli


def test_run_all_default_does_not_call_live_sec(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(cli, "ingest_demo", lambda **kwargs: calls.append("ingest_demo"))
    monkeypatch.setattr(cli, "ingest_ak", lambda **kwargs: calls.append("ingest_ak"))
    monkeypatch.setattr(cli, "ingest_tx", lambda **kwargs: calls.append("ingest_tx"))
    monkeypatch.setattr(cli, "ingest_ree", lambda **kwargs: calls.append("ingest_ree"))
    monkeypatch.setattr(cli, "ingest_sec", lambda **kwargs: calls.append("ingest_sec"))
    monkeypatch.setattr(cli, "ingest_sec_live", lambda **kwargs: calls.append("ingest_sec_live"))
    monkeypatch.setattr(cli, "generate_alerts", lambda **kwargs: calls.append("generate_alerts"))
    monkeypatch.setattr(cli, "report", lambda **kwargs: calls.append("report"))
    monkeypatch.setattr(cli, "opportunities", lambda **kwargs: calls.append("opportunities"))

    cli.run_all(hours=72, report_hours=24, top_n=25, report_file=None)

    assert "ingest_sec" in calls
    assert "ingest_sec_live" not in calls
    assert calls[-1] == "opportunities"


def test_run_all_calls_live_sec_when_enabled(monkeypatch):
    calls: list[tuple[str, dict]] = []

    monkeypatch.setattr(cli, "ingest_demo", lambda **kwargs: calls.append(("ingest_demo", kwargs)))
    monkeypatch.setattr(cli, "ingest_ak", lambda **kwargs: calls.append(("ingest_ak", kwargs)))
    monkeypatch.setattr(cli, "ingest_tx", lambda **kwargs: calls.append(("ingest_tx", kwargs)))
    monkeypatch.setattr(cli, "ingest_ree", lambda **kwargs: calls.append(("ingest_ree", kwargs)))
    monkeypatch.setattr(cli, "ingest_sec", lambda **kwargs: calls.append(("ingest_sec", kwargs)))
    monkeypatch.setattr(cli, "generate_alerts", lambda **kwargs: calls.append(("generate_alerts", kwargs)))
    monkeypatch.setattr(cli, "report", lambda **kwargs: calls.append(("report", kwargs)))
    monkeypatch.setattr(cli, "opportunities", lambda **kwargs: calls.append(("opportunities", kwargs)))

    def _fake_ingest_sec_live(**kwargs):
        calls.append(("ingest_sec_live", kwargs))

    monkeypatch.setattr(cli, "ingest_sec_live", _fake_ingest_sec_live)

    cli.run_all(
        hours=48,
        report_hours=12,
        top_n=9,
        report_file="out.json",
        sec_live=True,
        sec_live_max_filings_per_company=7,
        sec_live_user_agent="OGREE Test (test@example.com)",
        sec_live_timeout_s=33,
        sec_live_request_delay_s=0.45,
        sec_live_max_retries=5,
        sec_live_backoff_base_s=1.25,
        sec_live_universe_path="config/universe.yaml",
    )

    live_calls = [c for c in calls if c[0] == "ingest_sec_live"]
    assert len(live_calls) == 1
    _, kwargs = live_calls[0]
    assert kwargs["max_filings_per_company"] == 7
    assert kwargs["user_agent"] == "OGREE Test (test@example.com)"
    assert kwargs["timeout_s"] == 33
    assert kwargs["request_delay_s"] == 0.45
    assert kwargs["max_retries"] == 5
    assert kwargs["backoff_base_s"] == 1.25
    assert kwargs["universe_path"] == "config/universe.yaml"


def test_ingest_sec_live_rejects_invalid_knobs():
    with pytest.raises(typer.BadParameter) as exc_info:
        cli.ingest_sec_live(
            max_filings_per_company=20,
            user_agent="UA",
            timeout_s=20,
            request_delay_s=-0.1,
            max_retries=3,
            backoff_base_s=1.0,
            universe_path="config/universe.yaml",
        )
    assert "request_delay_s" in str(exc_info.value)


def test_run_all_rejects_invalid_sec_live_params_before_ingest(monkeypatch):
    monkeypatch.setattr(cli, "ingest_demo", lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not run")))
    with pytest.raises(typer.BadParameter) as exc_info:
        cli.run_all(
            hours=48,
            report_hours=12,
            top_n=9,
            report_file=None,
            sec_live=True,
            sec_live_max_filings_per_company=7,
            sec_live_user_agent="OGREE Test (test@example.com)",
            sec_live_timeout_s=33,
            sec_live_request_delay_s=0.2,
            sec_live_max_retries=-1,
            sec_live_backoff_base_s=1.25,
            sec_live_universe_path="config/universe.yaml",
        )
    assert "max_retries" in str(exc_info.value)
