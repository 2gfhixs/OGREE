from __future__ import annotations

from ogree_alpha.entity_resolution import resolve_company

from datetime import datetime, timezone
from typing import Any, Dict

from ogree_alpha.hashing import sha256_hex
from ogree_alpha.chain_view import compute_chain_scores, load_recent_events
from ogree_alpha.db.repo import insert_alert


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def tier_for_score(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.5:
        return "medium"
    if score >= 0.3:
        return "low"
    return ""


def build_alert(row: Dict[str, Any], utc_date: str, company_id: str | None = None) -> Dict[str, Any]:
    lineage_id = row["lineage_id"]
    tier = tier_for_score(float(row["score"]))
    last_event_time = row.get("last_event_time")  # datetime (for DB column)

    alert_id = sha256_hex(f"chain_progression|AK|{lineage_id}|{utc_date}")[:24]
    canonical_doc_id = sha256_hex(f"chain_progression|{lineage_id}|{last_event_time}")[:24]

    # JSON-safe value for embedding
    last_event_time_iso = _dt_to_iso(last_event_time)

    evidence_pointer = {
        "lineage_id": lineage_id,
        "permit_id": row.get("permit_id"),
        "operator": row.get("operator"),
        "company": row.get("company"),
        "region": row.get("region"),
        "last_event_time": last_event_time_iso,
    }

    score_summary = {
        "score": float(row["score"]),
        "has_permit": bool(row.get("has_permit")),
        "has_spud": bool(row.get("has_spud")),
        "has_well": bool(row.get("has_well")),
        "has_production": bool(row.get("has_production")),
        "has_insider_buy": bool(row.get("has_insider_buy")),
        "has_insider_buy_cluster": bool(row.get("has_insider_buy_cluster")),
        "convergence_score": int(row.get("convergence_score") or 0),
        "convergence_categories": list(row.get("convergence_categories") or []),
    }

    actor = row.get("operator") or row.get("company") or "unknown"
    summary = (
        f"[{tier.upper()}] chain progression "
        f"{row.get('permit_id') or lineage_id} ({actor}, {row.get('region')}) "
        f"score={score_summary['score']}"
    )
    if score_summary["convergence_score"] >= 3:
        categories_text = ",".join(score_summary["convergence_categories"])
        summary += f" convergence={score_summary['convergence_score']} [{categories_text}]"

    # Ensure JSONB field doesn't contain datetime objects
    row_safe = dict(row)
    row_safe["last_event_time"] = last_event_time_iso

    return {
        "alert_id": alert_id,
        "tier": tier,
        "event_type": "chain_progression",
        "event_time": last_event_time,
        "ingest_time": _now_utc(),
        "company_id": company_id,
        "asset_id": None,
        "canonical_doc_id": canonical_doc_id,
        "evidence_pointer": evidence_pointer,
        "score_summary": score_summary,
        "summary": summary,
        "details": {"row": row_safe},
        "regime_context": None,
    }


def generate_and_insert_alerts(hours: int = 72, top_n: int = 25) -> int:
    events = load_recent_events(hours=hours)
    rows = compute_chain_scores(events)[:top_n]

    utc_date = _now_utc().date().isoformat()
    inserted = 0
    for r in rows:
        # Prefer adapter-provided company_id; otherwise resolve from chain fields.
        company_id = r.get("company_id")
        if company_id is None:
            resolved = resolve_company(name=r.get("company"), operator=r.get("operator"))
            company_id = resolved.company_id
        tier = tier_for_score(float(r["score"]))
        if not tier:
            continue
        alert = build_alert(r, utc_date=utc_date, company_id=company_id)
        did_insert = insert_alert(alert)
        inserted += 1 if did_insert else 0
    return inserted
