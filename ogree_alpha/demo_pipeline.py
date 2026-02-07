from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from ogree_alpha.hashing import alert_id as make_alert_id
from ogree_alpha.hashing import canonical_doc_id as make_canonical_doc_id
from ogree_alpha.hashing import content_hash as make_content_hash
from ogree_alpha.db.repo import insert_alert, insert_raw_event


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # accept "Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def load_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def score_event(payload: Dict[str, Any]) -> float:
    t = (payload.get("type") or "").lower()
    if t == "lease_grant":
        return 0.35
    if t == "permit_filed":
        return 0.55
    if t == "drill_result":
        # crude "positive" heuristic
        txt = json.dumps(payload).lower()
        return 0.85 if ("shows" in txt or "flow" in txt) else 0.65
    return 0.25


def tier_from_score(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def event_type(payload: Dict[str, Any]) -> str:
    t = payload.get("type")
    return str(t) if t else "generic"


def build_alert(raw_event_db: Dict[str, Any]) -> Dict[str, Any]:
    payload = raw_event_db["payload_json"]
    score = score_event(payload)
    tier = tier_from_score(score)
    etype = event_type(payload)

    canon = raw_event_db["canonical_doc_id"]
    aid = make_alert_id(canon, tier=tier, event_type=etype)

    region = payload.get("region")
    headline = f"{etype} ({region})" if region else etype

    return {
        "alert_id": aid,
        "tier": tier,
        "event_type": etype,
        "event_time": raw_event_db.get("event_time"),
        "ingest_time": raw_event_db.get("ingest_time"),
        "company_id": "COMPANY_1",
        "asset_id": None,
        "canonical_doc_id": canon,
        "evidence_pointer": {
            "source": raw_event_db["source_system"],
            "doc_id": canon,
            "meta": {"source_event_id": raw_event_db.get("source_event_id")},
        },
        "score_summary": {"score": score, "components": {"demo_rule": score}},
        "summary": headline,
        "details": payload,
        "regime_context": None,
    }


def ingest_and_alert(path: str) -> List[Dict[str, Any]]:
    emitted: List[Dict[str, Any]] = []

    for row in load_jsonl(path):
        source_system = row["source_system"]
        payload_json = row["payload_json"]

        ch = make_content_hash(payload_json)
        canon = make_canonical_doc_id(source_system, ch)

        raw_event = {
            "source_system": source_system,
            "source_event_id": row.get("source_event_id"),
            "event_time": _parse_dt(row.get("event_time")),
            "ingest_time": datetime.now(timezone.utc),
            "payload_json": payload_json,
            "content_hash": ch,
            "canonical_doc_id": canon,
        }

        # write to event_log
        inserted, db_id = insert_raw_event(raw_event)

        # create + write alert
        alert = build_alert(raw_event)
        did_insert = insert_alert(alert)

        emitted.append(
            {
                "raw_event": {"inserted": inserted, "db_id": db_id, "canonical_doc_id": canon},
                "alert": {"inserted": did_insert, **alert},
            }
        )

    return emitted


def main() -> None:
    out = ingest_and_alert("sample_data/raw_events.jsonl")
    for item in out:
        print(json.dumps(item, default=str))


if __name__ == "__main__":
    main()
