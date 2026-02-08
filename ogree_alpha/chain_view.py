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
TX_WELL_TYPES = {"completion_reported", "well_completion", "drill_result", "well_record"}


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
        "has_well": False,
        "operator": None,
        "region": None,
        "permit_id": None,
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

        t = pj.get("type")
        region = (pj.get("region") or "").strip()

        # baseline AK-ish semantics
        if t == "permit_filed":
            b["has_permit"] = True
        elif t in ("well_record", "completion_reported"):
            b["has_well"] = True

        # Phase 9C: TX semantics (treat several TX event types as permit/well evidence)
        if region.lower() == "texas":
            if t in TX_PERMIT_TYPES:
                b["has_permit"] = True
            if t in TX_WELL_TYPES:
                b["has_well"] = True


    rows_out: List[Dict[str, Any]] = []
    for lineage_id, b in buckets.items():
        score = (0.4 if b["has_permit"] else 0.0) + (0.6 if b["has_well"] else 0.0)
        rows_out.append(
            {
                "lineage_id": lineage_id,
                "score": score,
                "has_permit": bool(b["has_permit"]),
                "has_well": bool(b["has_well"]),
                "operator": b["operator"],
                "region": b["region"],
                "permit_id": b["permit_id"],
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
