from __future__ import annotations

import typer

from ogree_alpha import cli


def test_email_report_writes_eml_without_sending(monkeypatch, tmp_path):
    written = {"path": None}

    monkeypatch.setattr(
        "ogree_alpha.report_twice_daily.render_report",
        lambda hours=24, top_n=10: {"subject": "S", "text": "T", "html": "<p>H</p>"},
    )
    monkeypatch.setattr(
        "ogree_alpha.email_report.build_report_email_message",
        lambda **kwargs: object(),
    )

    def _fake_save(message, output_path):
        written["path"] = output_path
        return output_path

    monkeypatch.setattr("ogree_alpha.email_report.save_email_message", _fake_save)
    monkeypatch.setattr(
        "ogree_alpha.email_report.send_email_via_smtp",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("send should not be called")),
    )

    out = tmp_path / "demo.eml"
    cli.email_report(
        to="team@example.com",
        from_email="ogree@example.com",
        hours=24,
        top_n=10,
        output=str(out),
        send=False,
        smtp_host=None,
        smtp_port=587,
        smtp_user=None,
        smtp_password=None,
        smtp_tls=True,
        smtp_ssl=False,
    )
    assert written["path"] == str(out)


def test_email_report_requires_recipients():
    try:
        cli.email_report(
            to="",
            from_email="ogree@example.com",
            hours=24,
            top_n=10,
            output="out.eml",
            send=False,
            smtp_host=None,
            smtp_port=587,
            smtp_user=None,
            smtp_password=None,
            smtp_tls=True,
            smtp_ssl=False,
        )
    except typer.BadParameter as exc:
        assert "recipient" in str(exc).lower()
    else:
        raise AssertionError("Expected typer.BadParameter for empty recipients")
