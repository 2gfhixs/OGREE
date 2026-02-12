from __future__ import annotations

from ogree_alpha.email_report import build_report_email_message, save_email_message


def test_build_report_email_message_has_text_and_html_parts():
    report = {
        "subject": "OGREE Demo",
        "text": "Text body",
        "html": "<p>HTML body</p>",
    }
    msg = build_report_email_message(
        report_obj=report,
        from_email="ogree@example.com",
        to_emails=["team@example.com", "ops@example.com"],
    )
    assert msg["Subject"] == "OGREE Demo"
    assert "team@example.com" in msg["To"]
    assert msg.get_body(preferencelist=("plain",)) is not None
    assert msg.get_body(preferencelist=("html",)) is not None


def test_save_email_message_writes_eml(tmp_path):
    report = {
        "subject": "OGREE Demo",
        "text": "Text body",
        "html": "<p>HTML body</p>",
    }
    msg = build_report_email_message(
        report_obj=report,
        from_email="ogree@example.com",
        to_emails=["team@example.com"],
    )
    out_path = tmp_path / "demo.eml"
    written = save_email_message(msg, str(out_path))
    assert written.endswith("demo.eml")
    assert out_path.exists()
    content = out_path.read_text(encoding="utf-8", errors="replace")
    assert "Subject: OGREE Demo" in content
