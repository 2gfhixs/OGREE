from __future__ import annotations

from email.message import EmailMessage
from pathlib import Path
import smtplib
from typing import Any, Mapping, Sequence


def build_report_email_message(
    *,
    report_obj: Mapping[str, Any],
    from_email: str,
    to_emails: Sequence[str],
) -> EmailMessage:
    subject = str(report_obj.get("subject") or "OGREE Alpha Report")
    text = str(report_obj.get("text") or "")
    html = str(report_obj.get("html") or "<p>(empty report)</p>")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg.set_content(text)
    msg.add_alternative(html, subtype="html")
    return msg


def save_email_message(message: EmailMessage, output_path: str) -> str:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(message.as_bytes())
    return str(path)


def send_email_via_smtp(
    *,
    message: EmailMessage,
    host: str,
    port: int = 587,
    username: str | None = None,
    password: str | None = None,
    use_tls: bool = True,
    use_ssl: bool = False,
    timeout_s: int = 30,
) -> None:
    if use_ssl:
        with smtplib.SMTP_SSL(host, port, timeout=timeout_s) as client:
            if username:
                client.login(username, password or "")
            client.send_message(message)
        return

    with smtplib.SMTP(host, port, timeout=timeout_s) as client:
        if use_tls:
            client.starttls()
        if username:
            client.login(username, password or "")
        client.send_message(message)
