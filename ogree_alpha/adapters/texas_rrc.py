from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from ogree_alpha.hashing import sha256_hex
from ogree_alpha.db.repo import insert_raw_event


SOURCE_SYSTEM = "tx_rrc"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        s = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _normalize_lineage_id(payload: Mapping[str, Any]) -> str | None:
    # Texas: prefer API number if present; else permit_no
    api = payload.get("api")
    if isinstance(api, str) and api.strip():
        return api.strip()
    permit_no = payload.get("permit_no")
    if isinstance(permit_no, str) and permit_no.strip():
        return f"permit:{permit_no.strip()}"
    return None


def iter_fixture_events(path: str = "sample_data/texas/rrc_raw_events.jsonl") -> Iterable[Dict[str, Any]]:
    p = Path(path)
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if not isinstance(obj, dict):
            continue
        yield obj


def ingest_fixture_to_db(path: str = "sample_data/texas/rrc_raw_events.jsonl") -> int:
    inserted = 0
    for obj in iter_fixture_events(path):
        payload = obj.get("payload_json") or {}
        if not isinstance(payload, dict):
            payload = {}

        lineage_id = _normalize_lineage_id(payload)
        if lineage_id:
            payload = dict(payload)
            payload["lineage_id"] = lineage_id

        event_time = _parse_dt(obj.get("event_time"))

        # Required DB fields
        raw_event = {
            "source_system": SOURCE_SYSTEM,
            "source_event_id": obj.get("source_event_id"),
            "event_time": event_time,
            "ingest_time": _now_utc(),
            "payload_json": payload,
            "content_hash": sha256_hex(json.dumps(payload, sort_keys=True, default=str)),
            "canonical_doc_id": f"{SOURCE_SYSTEM}:{sha256_hex(f'{obj.get('source_event_id')}|{payload.get('type')}')[:16]}",
        }
        did_insert, _id = insert_raw_event(raw_event)
        inserted += 1 if did_insert else 0
    return inserted
