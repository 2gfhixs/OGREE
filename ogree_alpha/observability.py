from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List

from sqlalchemy import func, select

from ogree_alpha.chain_view import compute_chain_scores, load_recent_events
from ogree_alpha.db.models import Alert, EventLog
from ogree_alpha.db.session import get_session


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 2)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def load_recent_alert_rows(hours: int = 24, limit: int = 1000) -> List[Dict[str, Any]]:
    cutoff = _now_utc() - timedelta(hours=hours)
    rows: List[Dict[str, Any]] = []
    with get_session() as session:
        stmt = (
            select(Alert.tier, Alert.company_id, Alert.score_summary, Alert.event_type, Alert.ingest_time)
            .where(Alert.ingest_time >= cutoff)
            .order_by(Alert.ingest_time.desc())
            .limit(limit)
        )
        for tier, company_id, score_summary, event_type, ingest_time in session.execute(stmt).all():
            rows.append(
                {
                    "tier": tier,
                    "company_id": company_id,
                    "score_summary": score_summary or {},
                    "event_type": event_type,
                    "ingest_time": ingest_time,
                }
            )
    return rows


def load_source_counts(hours: int = 72) -> Dict[str, int]:
    cutoff = _now_utc() - timedelta(hours=hours)
    out: Dict[str, int] = {}
    with get_session() as session:
        stmt = (
            select(EventLog.source_system, func.count())
            .where(EventLog.ingest_time >= cutoff)
            .group_by(EventLog.source_system)
        )
        for source_system, count_value in session.execute(stmt).all():
            out[str(source_system)] = int(count_value)
    return dict(sorted(out.items(), key=lambda kv: kv[0]))


def summarize_chain_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows_list = list(rows)
    total = len(rows_list)
    if total == 0:
        return {
            "lineages": 0,
            "avg_score": 0.0,
            "lineages_high_score": 0,
            "lineages_with_insider_signal": 0,
            "lineages_convergence_watch": 0,
            "lineages_convergence_3plus": 0,
            "lineages_with_company_id": 0,
            "lineage_company_resolution_rate_pct": 0.0,
        }

    avg_score = round(sum(_safe_float(r.get("score")) for r in rows_list) / float(total), 4)
    high_score = sum(_safe_float(r.get("score")) >= 0.8 for r in rows_list)
    insider = sum(bool(r.get("has_insider_buy")) for r in rows_list)
    conv_watch = sum(int(r.get("convergence_score") or 0) == 2 for r in rows_list)
    conv_3plus = sum(int(r.get("convergence_score") or 0) >= 3 for r in rows_list)
    with_company_id = sum(bool(r.get("company_id")) for r in rows_list)

    return {
        "lineages": total,
        "avg_score": avg_score,
        "lineages_high_score": int(high_score),
        "lineages_with_insider_signal": int(insider),
        "lineages_convergence_watch": int(conv_watch),
        "lineages_convergence_3plus": int(conv_3plus),
        "lineages_with_company_id": int(with_company_id),
        "lineage_company_resolution_rate_pct": _pct(int(with_company_id), total),
    }


def summarize_alert_rows(alert_rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    alerts = list(alert_rows)
    total = len(alerts)

    tiers = Counter((str(a.get("tier") or "").lower()) for a in alerts)
    tier_counts = {
        "high": int(tiers.get("high", 0)),
        "medium": int(tiers.get("medium", 0)),
        "low": int(tiers.get("low", 0)),
    }

    with_company_id = sum(bool(a.get("company_id")) for a in alerts)
    conv_3plus = sum(
        int((a.get("score_summary") or {}).get("convergence_score") or 0) >= 3
        for a in alerts
    )
    avg_score = 0.0
    if total > 0:
        avg_score = round(
            sum(_safe_float((a.get("score_summary") or {}).get("score")) for a in alerts) / float(total),
            4,
        )

    return {
        "alerts": total,
        "avg_score": avg_score,
        "alerts_with_company_id": int(with_company_id),
        "alert_company_resolution_rate_pct": _pct(int(with_company_id), total),
        "alerts_convergence_3plus": int(conv_3plus),
        "tier_counts": tier_counts,
    }


def compute_health_snapshot(hours: int = 72, alert_hours: int = 24) -> Dict[str, Any]:
    events = load_recent_events(hours=hours)
    chain_rows = compute_chain_scores(events)
    alerts = load_recent_alert_rows(hours=alert_hours)
    source_counts = load_source_counts(hours=hours)

    return {
        "generated_at": _now_utc().isoformat().replace("+00:00", "Z"),
        "event_window_hours": int(hours),
        "alert_window_hours": int(alert_hours),
        "source_counts": source_counts,
        "chain": summarize_chain_rows(chain_rows),
        "alerts": summarize_alert_rows(alerts),
    }


def render_text(snapshot: Dict[str, Any]) -> str:
    chain = snapshot.get("chain") or {}
    alerts = snapshot.get("alerts") or {}
    source_counts = snapshot.get("source_counts") or {}

    lines = [
        f"Generated:            {snapshot.get('generated_at')}",
        f"Event window (hours): {snapshot.get('event_window_hours')}",
        f"Alert window (hours): {snapshot.get('alert_window_hours')}",
        "",
        "Source counts:",
    ]
    if source_counts:
        for source, count in sorted(source_counts.items()):
            lines.append(f"  - {source}: {count}")
    else:
        lines.append("  - none")

    lines.extend(
        [
            "",
            "Chain health:",
            f"  - lineages: {chain.get('lineages', 0)}",
            f"  - avg score: {chain.get('avg_score', 0.0)}",
            f"  - high-score lineages (>=0.8): {chain.get('lineages_high_score', 0)}",
            f"  - insider lineages: {chain.get('lineages_with_insider_signal', 0)}",
            f"  - convergence watch (2): {chain.get('lineages_convergence_watch', 0)}",
            f"  - convergence 3+: {chain.get('lineages_convergence_3plus', 0)}",
            f"  - lineage company resolution: {chain.get('lineage_company_resolution_rate_pct', 0.0)}%",
            "",
            "Alert health:",
            f"  - alerts: {alerts.get('alerts', 0)}",
            f"  - avg score: {alerts.get('avg_score', 0.0)}",
            f"  - convergence alerts 3+: {alerts.get('alerts_convergence_3plus', 0)}",
            f"  - alert company resolution: {alerts.get('alert_company_resolution_rate_pct', 0.0)}%",
            f"  - tiers: {json.dumps(alerts.get('tier_counts', {}), sort_keys=True)}",
        ]
    )
    return "\n".join(lines)
