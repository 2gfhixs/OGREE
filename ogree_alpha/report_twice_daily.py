from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from sqlalchemy import select

from ogree_alpha.db.models import Alert
from ogree_alpha.db.session import get_session


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_recent_alerts(hours: int = 12) -> List[Dict[str, Any]]:
    cutoff = _now_utc() - timedelta(hours=hours)
    out: List[Dict[str, Any]] = []
    with get_session() as session:
        stmt = (
            select(
                Alert.tier,
                Alert.event_type,
                Alert.event_time,
                Alert.ingest_time,
                Alert.canonical_doc_id,
                Alert.evidence_pointer,
                Alert.score_summary,
                Alert.summary,
                Alert.details,
            )
            .where(Alert.ingest_time >= cutoff)
            .order_by(Alert.ingest_time.desc())
        )
        for row in session.execute(stmt).all():
            out.append(
                {
                    "tier": row[0],
                    "event_type": row[1],
                    "event_time": row[2],
                    "ingest_time": row[3],
                    "canonical_doc_id": row[4],
                    "evidence_pointer": row[5],
                    "score_summary": row[6],
                    "summary": row[7],
                    "details": row[8],
                }
            )
    return out


def render_report(hours: int = 12, top_n: int = 10) -> Dict[str, str]:
    today = _now_utc().date().isoformat()
    subject = f"OGREE Alpha — Top Alerts (AK) — {today}"

    alerts = load_recent_alerts(hours=hours)
    if not alerts:
        text = "No new alerts in last 12h."
        html = "<p>No new alerts in last 12h.</p>"
        return {"subject": subject, "text": text, "html": html}

    order = ["high", "medium", "low"]
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for a in alerts:
        buckets[str(a.get("tier") or "low")].append(a)

    lines: List[str] = []
    html_parts: List[str] = []

    for tier in order:
        items = buckets.get(tier, [])[:top_n]
        if not items:
            continue
        lines.append(f"{tier.upper()}:")
        html_parts.append(f"<h3>{tier.upper()}</h3><ul>")
        for it in items:
            score = (it.get("score_summary") or {}).get("score")
            et = it.get("event_time")
            lines.append(f"  - {it.get('summary')} (event_time={et}, score={score})")
            html_parts.append(f"<li>{it.get('summary')} (event_time={et}, score={score})</li>")
        html_parts.append("</ul>")
        lines.append("")

    return {"subject": subject, "text": "\n".join(lines).strip(), "html": "\n".join(html_parts).strip()}


def main() -> None:
    print(json.dumps(render_report(), default=str))


if __name__ == "__main__":
    main()
