from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from ogree_alpha.hashing import sha256_hex
from ogree_alpha.db.repo import insert_raw_event

SOURCE_SYSTEM = "alaska_permits"
LIVE_URL: str = "https://example.invalid/alaska/permits"  # placeholder

DEFAULT_ZIP_FIXTURE = "sample_data/alaska/wellhistory_fixture.zip"
RAW_EVENTS_FALLBACK = "sample_data/raw_events.jsonl"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        v = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(v)
        except ValueError:
            return None
    return None


def fetch_live(url: str = LIVE_URL, timeout_s: int = 10) -> Dict[str, Any]:
    """
    Placeholder live fetch (opt-in later). Kept minimal.
    """
    try:
        import requests  # type: ignore
    except Exception as e:
        raise RuntimeError("requests not installed") from e

    r = requests.get(url, timeout=timeout_s)
    if r.status_code != 200:
        raise RuntimeError(f"fetch_live status={r.status_code}")
    return r.json()


def _lineage_id(permit_id: str, operator: str, region: str) -> str:
    return sha256_hex(f"AK|{permit_id}|{operator}|{region}")[:20]


def normalize_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    permit_id = str(row.get("permit_id") or row.get("permit") or row.get("permit_number") or "").strip() or "UNKNOWN"
    operator = str(row.get("operator") or row.get("lessee") or "UNKNOWN").strip() or "UNKNOWN"
    region = str(row.get("region") or row.get("state") or "Alaska").strip() or "Alaska"
    event_time = _parse_dt(row.get("event_time") or row.get("date") or row.get("filed_at") or row.get("reported_at"))

    payload: Dict[str, Any] = {
        "type": "permit_filed",
        "jurisdiction": "AK",
        "source": SOURCE_SYSTEM,
        "permit_id": permit_id,
        "operator": operator,
        "region": region,
        "activity": row.get("activity") or row.get("description") or "exploration",
        "event_time": event_time.isoformat().replace("+00:00", "Z") if event_time else None,
    }
    payload["lineage_id"] = _lineage_id(permit_id=permit_id, operator=operator, region=region)
    return payload


def raw_events_from_payloads(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in payloads:
        source_event_id = p.get("permit_id")
        source_event_id = str(source_event_id) if source_event_id else None

        content_hash = sha256_hex(json.dumps(p, sort_keys=True, separators=(",", ":")))
        canonical_doc_id = sha256_hex(f"{SOURCE_SYSTEM}|{p.get('type')}|{p.get('lineage_id')}|{source_event_id}")[:24]
        event_time = _parse_dt(p.get("event_time"))

        out.append(
            {
                "source_system": SOURCE_SYSTEM,
                "source_event_id": source_event_id,
                "event_time": event_time,
                "ingest_time": _now_utc(),
                "payload_json": p,
                "content_hash": content_hash,
                "canonical_doc_id": canonical_doc_id,
            }
        )
    return out


def _load_fallback_jsonl(path: str = RAW_EVENTS_FALLBACK) -> List[Dict[str, Any]]:
    """
    Fallback: ingest permit_filed events from sample_data/raw_events.jsonl
    (and normalize shape to include lineage_id if missing).
    """
    p = Path(path)
    if not p.exists():
        return []
    payloads: List[Dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        pj = obj.get("payload_json") or {}
        if pj.get("type") != "permit_filed":
            continue

        # Ensure required AK linkage fields exist for chain scoring
        permit_id = str(pj.get("permit_id") or pj.get("permit") or pj.get("permit_number") or "UNKNOWN")
        operator = str(pj.get("operator") or pj.get("lessee") or "UNKNOWN")
        region = str(pj.get("region") or "Alaska")

        pj = dict(pj)
        pj.setdefault("permit_id", permit_id)
        pj.setdefault("operator", operator)
        pj.setdefault("region", region)
        pj.setdefault("source", SOURCE_SYSTEM)
        pj.setdefault("jurisdiction", "AK")
        pj.setdefault("lineage_id", _lineage_id(permit_id=permit_id, operator=operator, region=region))

        payloads.append(pj)
    return payloads


def ingest_zip_fixture_to_db(fixture_path: str = DEFAULT_ZIP_FIXTURE) -> int:
    """
    Phase 6 minimal ingestion:
    - Zip parsing intentionally stubbed for now; we fall back to JSONL.
    - Returns number of NEW events inserted.
    """
    inserted = 0

    # NOTE: zip parsing is intentionally stubbed; rely on JSONL fallback until fixture format is standardized.
    if not Path(fixture_path).exists():
        payloads = _load_fallback_jsonl()
    else:
        payloads = _load_fallback_jsonl()

    raw_events = raw_events_from_payloads(payloads)
    for ev in raw_events:
        did_insert, _ = insert_raw_event(ev)
        inserted += 1 if did_insert else 0
    return inserted
