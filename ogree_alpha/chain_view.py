from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from ogree_alpha.db.models import EventLog
from ogree_alpha.db.session import get_session

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_recent_events(hours: int = 72) -> List[Dict[str, Any]]:
    """
    Load recent raw events from DB. Returns a list of dicts containing:
    - payload_json (dict)
    - event_time (datetime|None)
    - ingest_time (datetime)
    """
    cutoff = _now_utc() - timedelta(hours=hours)
    rows: List[Dict[str, Any]] = []

    with get_session() as session:
        stmt = (
            select(EventLog.payload_json, EventLog.event_time, EventLog.ingest_time)
            .where(EventLog.ingest_time >= cutoff)
            .order_by(EventLog.ingest_time.desc())
        )
        for payload_json, event_time, ingest_time in session.execute(stmt).all():
            rows.append(
                {
                    "payload_json": payload_json,
                    "event_time": event_time,
                    "ingest_time": ingest_time,
                }
            )
    return rows
TX_PERMIT_TYPES = {"permit_filed", "permit_issued", "drilling_permit"}
TX_SPUD_TYPES = {"spud_reported"}
TX_WELL_TYPES = {"completion_reported", "well_completion", "drill_result", "well_record"}
TX_PRODUCTION_TYPES = {"production_reported"}

# REE/U lifecycle stages
REE_U_CLAIMS_TYPES = {"claims_staked", "exploration_permit"}
REE_U_DRILL_TYPES = {"drill_assay"}
REE_U_RESOURCE_TYPES = {"resource_estimate"}
REE_U_STUDY_TYPES = {"pea_published", "pfs_published", "feasibility_study"}
REE_U_DEAL_TYPES = {"financing_closed", "financing_announced", "offtake_agreement"}
REE_U_POLICY_TYPES = {"policy_designation"}


def compute_chain_scores(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Group events by lineage_id and score progress:
      permit_filed => +0.4
      well_record  => +0.6
    Output rows sorted by score desc with keys:
      lineage_id, score, has_permit, has_well, operator, region, permit_id, last_event_time
    """
    buckets: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "has_permit": False,
        "has_spud": False,
        "has_well": False,
        "has_production": False,
        "has_claims": False,
        "has_drill_assay": False,
        "has_resource": False,
        "has_study": False,
        "has_deal": False,
        "has_policy": False,
        "operator": None,
        "region": None,
        "permit_id": None,
        "field": None,
        "county": None,
        "ip_boed": None,
        "commodity": None,
        "company": None,
        "project": None,
        "tickers": None,
        "last_event_time": None,
    })

    for e in events:
        pj = e.get("payload_json") or {}
        lineage_id = pj.get("lineage_id")
        if not lineage_id:
            continue

        b = buckets[str(lineage_id)]
        et = e.get("event_time") or e.get("ingest_time")

        # update last_event_time
        if b["last_event_time"] is None or (et and et > b["last_event_time"]):
            b["last_event_time"] = et

        # carry context
        b["operator"] = b["operator"] or pj.get("operator")
        b["region"] = b["region"] or pj.get("region")
        b["permit_id"] = b["permit_id"] or pj.get("permit_id")
        b["field"] = b["field"] or pj.get("field")
        b["county"] = b["county"] or pj.get("county")
        # carry best IP rate seen
        ip = pj.get("ip_boed")
        if ip is not None:
            try:
                ip_f = float(ip)
                if b["ip_boed"] is None or ip_f > b["ip_boed"]:
                    b["ip_boed"] = ip_f
            except (ValueError, TypeError):
                pass

        t = pj.get("type")
        region = (pj.get("region") or "").strip()

        # baseline AK-ish semantics
        if t == "permit_filed":
            b["has_permit"] = True
        elif t in ("well_record", "completion_reported"):
            b["has_well"] = True

        # Phase 9: TX semantics (richer event type coverage)
        if region.lower() == "texas":
            if t in TX_PERMIT_TYPES:
                b["has_permit"] = True
            if t in TX_SPUD_TYPES:
                b["has_spud"] = True
            if t in TX_WELL_TYPES:
                b["has_well"] = True
            if t in TX_PRODUCTION_TYPES:
                b["has_production"] = True

        # Phase 10: REE/U lifecycle
        commodity = (pj.get("commodity") or "").strip().lower()
        if commodity in ("ree", "uranium"):
            b["commodity"] = b["commodity"] or pj.get("commodity")
            b["company"] = b["company"] or pj.get("company")
            b["project"] = b["project"] or pj.get("project")
            if not b["tickers"] and pj.get("tickers"):
                b["tickers"] = pj.get("tickers")
            if t in REE_U_CLAIMS_TYPES:
                b["has_claims"] = True
                b["has_permit"] = True  # also counts as permit-level for unified scoring
            if t in REE_U_DRILL_TYPES:
                b["has_drill_assay"] = True
                b["has_well"] = True  # maps to well-level in unified scoring
            if t in REE_U_RESOURCE_TYPES:
                b["has_resource"] = True
            if t in REE_U_STUDY_TYPES:
                b["has_study"] = True
            if t in REE_U_DEAL_TYPES:
                b["has_deal"] = True
            if t in REE_U_POLICY_TYPES:
                b["has_policy"] = True


    rows_out: List[Dict[str, Any]] = []
    for lineage_id, b in buckets.items():
        # Base score: O&G 4-stage chain
        score = (
            (0.3 if b["has_permit"] else 0.0)
            + (0.2 if b["has_spud"] else 0.0)
            + (0.3 if b["has_well"] else 0.0)
            + (0.2 if b["has_production"] else 0.0)
        )
        # REE/U bonus stages (additive — can push score above 1.0 for convergence)
        if b["has_resource"]:
            score += 0.15
        if b["has_study"]:
            score += 0.2
        if b["has_deal"]:
            score += 0.15
        if b["has_policy"]:
            score += 0.1

        rows_out.append(
            {
                "lineage_id": lineage_id,
                "score": round(score, 4),
                "has_permit": bool(b["has_permit"]),
                "has_spud": bool(b["has_spud"]),
                "has_well": bool(b["has_well"]),
                "has_production": bool(b["has_production"]),
                "has_claims": bool(b["has_claims"]),
                "has_drill_assay": bool(b["has_drill_assay"]),
                "has_resource": bool(b["has_resource"]),
                "has_study": bool(b["has_study"]),
                "has_deal": bool(b["has_deal"]),
                "has_policy": bool(b["has_policy"]),
                "operator": b["operator"],
                "region": b["region"],
                "permit_id": b["permit_id"],
                "field": b["field"],
                "county": b["county"],
                "ip_boed": b["ip_boed"],
                "commodity": b["commodity"],
                "company": b["company"],
                "project": b["project"],
                "tickers": b["tickers"],
                "last_event_time": b["last_event_time"],
            }
        )

    rows_out.sort(key=lambda r: r["score"], reverse=True)
    return rows_out


def main() -> None:
    events = load_recent_events(hours=72)
    rows = compute_chain_scores(events)[:25]
    for r in rows:
        # datetime -> iso
        out = dict(r)
        lt = out.get("last_event_time")
        if isinstance(lt, datetime):
            out["last_event_time"] = lt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        print(json.dumps(out, sort_keys=True))


if __name__ == "__main__":
    main()
