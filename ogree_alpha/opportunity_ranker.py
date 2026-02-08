from __future__ import annotations

from ogree_alpha.entity_resolution import resolve_company

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Mapping

from sqlalchemy import select

from ogree_alpha.db.models import Alert
from ogree_alpha.db.session import get_session
from ogree_alpha.universe import load_universe


@dataclass
class Opportunity:
    score: float
    tier: str
    company_id: Optional[str]
    tickers: List[str]
    asset_id: Optional[str]
    event_time: Optional[datetime]
    summary: str
    evidence: Dict[str, Any]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _tier_weight(tier: str) -> float:
    t = (tier or "").lower()
    if t == "high":
        return 1.0
    if t == "medium":
        return 0.6
    if t == "low":
        return 0.4
    return 0.0


def _recency_boost(event_time: Optional[datetime]) -> float:
    if not event_time:
        return 0.0
    age_h = (_now_utc() - event_time).total_seconds() / 3600.0
    if age_h <= 6:
        return 0.25
    if age_h <= 24:
        return 0.10
    return 0.02


def _get(obj: Any, key: str, default: Any = None) -> Any:
    # supports dicts and objects
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


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
                Alert.details,
            )
            .where(Alert.ingest_time >= cutoff)
            .order_by(Alert.ingest_time.desc())
            .limit(limit)
        )
        for row in session.execute(stmt).all():
            out.append(
                {
                    "tier": row[0],
                    "event_type": row[1],
                    "event_time": row[2],
                    "ingest_time": row[3],
                    "company_id": row[4],
                    "asset_id": row[5],
                    "summary": row[6],
                    "evidence_pointer": row[7] or {},
                    "score_summary": row[8] or {},
                    "details": row[9] or {},
                }
            )
    return out


def _companies_index(uni: Any) -> Dict[str, Any]:
    # uni can be a dict or an object; companies can be list[dict] or list[obj]
    companies_list = _get(uni, "companies", []) or []
    idx: Dict[str, Any] = {}
    for c in companies_list:
        cid = _get(c, "company_id")
        if cid:
            idx[str(cid)] = c
    return idx


def rank_opportunities(hours: int = 24, top_n: int = 15) -> List[Opportunity]:
    uni = load_universe()
    companies = _companies_index(uni)

    alerts = load_recent_alerts(hours=hours)
    ranked: List[Opportunity] = []

    for a in alerts:
        tier = str(a.get("tier") or "")
        base = _tier_weight(tier)

        ss = a.get("score_summary") or {}
        chain_score = float(ss.get("score") or 0.0)

        score = max(base, chain_score) + _recency_boost(a.get("event_time"))

        company_id = a.get("company_id")
        tickers: List[str] = []

        if company_id and str(company_id) in companies:
            c = companies[str(company_id)]
            tickers_val = _get(c, "tickers", []) or []
            tickers = list(tickers_val) if isinstance(tickers_val, list) else [str(tickers_val)]

        # Minimal Phase 7: if nothing mapped, but only one company exists, attach it
        if not tickers and companies:
            if len(companies) == 1:
                only = next(iter(companies.values()))
                tickers_val = _get(only, "tickers", []) or []
                tickers = list(tickers_val) if isinstance(tickers_val, list) else [str(tickers_val)]
                company_id = _get(only, "company_id")

        ranked.append(
            Opportunity(
                score=round(float(score), 4),
                tier=tier,
                company_id=str(company_id) if company_id is not None else None,
                tickers=[str(t) for t in tickers],
                asset_id=a.get("asset_id"),
                event_time=a.get("event_time"),
                summary=str(a.get("summary") or ""),
                evidence=dict(a.get("evidence_pointer") or {}),
            )
        )

    # Dedupe by summary+company+tier to avoid repeats from re-runs
    seen = set()
    uniq: List[Opportunity] = []
    for o in sorted(ranked, key=lambda x: x.score, reverse=True):
        key = (o.summary, o.company_id, o.tier)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(o)
        if len(uniq) >= top_n:
            break
    return uniq


def render_text(opps: List[Opportunity]) -> str:
    if not opps:
        return "No opportunities."
    lines = ["Rank | Score | Tier | Ticker(s) | Summary"]
    lines.append("-" * 110)
    for i, o in enumerate(opps, start=1):
        tick = ",".join(o.tickers) if o.tickers else "-"
        lines.append(f"{i:>4} | {o.score:<5} | {o.tier:<6} | {tick:<16} | {o.summary}")
    return "\n".join(lines)