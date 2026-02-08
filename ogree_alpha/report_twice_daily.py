from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from sqlalchemy import select

from ogree_alpha.db.models import Alert
from ogree_alpha.db.session import get_session
from ogree_alpha.opportunity_ranker import rank_opportunities, render_text as render_opps_text


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_recent_alerts(hours: int = 24, limit: int = 200) -> List[Dict[str, Any]]:
    cutoff = _now_utc() - timedelta(hours=hours)
    out: List[Dict[str, Any]] = []
    with get_session() as session:
        stmt = (
            select(
                Alert.tier,
                Alert.event_type,
                Alert.event_time,
                Alert.ingest_time,
                Alert.company_id,
                Alert.asset_id,
                Alert.summary,
                Alert.evidence_pointer,
                Alert.score_summary,
            )
            .where(Alert.ingest_time >= cutoff)
            .order_by(Alert.ingest_time.desc())
            .limit(limit)
        )
        for row in session.execute(stmt).all():
            out.append(
                {
                    "tier": (row[0] or "").lower(),
                    "event_type": row[1],
                    "event_time": row[2],
                    "ingest_time": row[3],
                    "company_id": row[4],
                    "asset_id": row[5],
                    "summary": row[6],
                    "evidence_pointer": row[7] or {},
                    "score_summary": row[8] or {},
                }
            )
    return out


def _escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def render_report(hours: int = 24, top_n: int = 10) -> Dict[str, Any]:
    today = _now_utc().date().isoformat()
    subject = f"OGREE Alpha — Top Alerts — {today}"

    # Phase 7: Opportunities section
    opps = rank_opportunities(hours=max(hours, 24), top_n=top_n)
    opps_table = render_opps_text(opps)
    opps_text = "Top Opportunities\n" + opps_table + "\n\n"
    opps_html = "<h2>Top Opportunities</h2><pre>" + _escape_html(opps_table) + "</pre>"

    # Alerts section
    alerts = load_recent_alerts(hours=hours)
    if not alerts:
        alerts_text = "No new alerts in the last window.\n"
        alerts_html = "<p><em>No new alerts in the last window.</em></p>"
    else:
        # Group by tier, and dedupe by (tier, summary)
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        seen = set()
        for a in alerts:
            key = (a["tier"], a.get("summary"))
            if key in seen:
                continue
            seen.add(key)
            grouped[a["tier"]].append(a)

        order = ["high", "medium", "low"]
        lines: List[str] = []
        html_parts: List[str] = []

        for t in order:
            items = grouped.get(t, [])
            if not items:
                continue
            lines.append(f"{t.upper()}:")
            html_parts.append(f"<h3>{t.upper()}</h3><ul>")
            for a in items:
                ss = a.get("score_summary") or {}
                sc = ss.get("score")
                et = a.get("event_time")
                summary = a.get("summary") or a.get("event_type") or "alert"
                lines.append(f"  - {summary} (event_time={et}, score={sc})")
                html_parts.append(f"<li>{_escape_html(str(summary))} (event_time={_escape_html(str(et))}, score={_escape_html(str(sc))})</li>")
            html_parts.append("</ul>")
            lines.append("")  # blank line between tiers

        alerts_text = "\n".join(lines).rstrip() + "\n"
        alerts_html = "\n".join(html_parts)

    text = opps_text + alerts_text
    html = opps_html + alerts_html

    return {"subject": subject, "text": text, "html": html}


def main() -> None:
    print(json.dumps(render_report(), default=str))


if __name__ == "__main__":
    main()
