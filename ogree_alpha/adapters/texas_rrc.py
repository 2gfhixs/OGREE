from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from ogree_alpha.hashing import sha256_hex
from ogree_alpha.db.repo import insert_raw_event


SOURCE_SYSTEM = "tx_rrc"

# Recognized event types and their canonical forms
_TYPE_ALIASES: Dict[str, str] = {
    "permit_filed": "permit_filed",
    "permit_issued": "permit_issued",
    "drilling_permit": "drilling_permit",
    "spud_reported": "spud_reported",
    "spud": "spud_reported",
    "drill_result": "drill_result",
    "drilling_result": "drill_result",
    "completion_reported": "completion_reported",
    "well_completion": "well_completion",
    "well_record": "well_record",
    "production_reported": "production_reported",
    "production": "production_reported",
    "plugging_report": "plugging_report",
    "p_and_a": "plugging_report",
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        s = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            pass
        # Try common RRC date formats: MM/DD/YYYY, YYYY-MM-DD
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
            try:
                return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _clean_str(value: Any) -> str | None:
    """Strip and normalize a string value; return None if empty."""
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    # Collapse internal whitespace
    value = " ".join(value.split())
    return value if value else None


def _normalize_api(api: Any) -> str | None:
    """Normalize API number to consistent format."""
    raw = _clean_str(api)
    if not raw:
        return None
    # Strip leading/trailing dashes, collapse multiple dashes
    raw = raw.strip("-")
    return raw if raw else None


def _normalize_lineage_id(payload: Mapping[str, Any]) -> str | None:
    """
    Derive lineage_id for chain scoring.
    Texas: prefer API number (groups all events for one well);
    fallback to permit_no if no API.
    """
    api = _normalize_api(payload.get("api"))
    if api:
        return f"TX:{api}"
    permit_no = _clean_str(payload.get("permit_no"))
    if permit_no:
        return f"TX:permit:{permit_no}"
    return None


def _normalize_type(raw_type: Any) -> str:
    """Map raw event type to canonical form."""
    if not raw_type or not isinstance(raw_type, str):
        return "unknown"
    key = raw_type.strip().lower().replace(" ", "_").replace("-", "_")
    return _TYPE_ALIASES.get(key, key)


def _canonicalize_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize and clean a TX RRC event payload."""
    p = dict(payload) if isinstance(payload, dict) else {}

    # Region: always "Texas"
    p["region"] = "Texas"

    # Normalize type
    p["type"] = _normalize_type(p.get("type"))

    # Clean operator
    p["operator"] = _clean_str(p.get("operator"))

    # Normalize API number
    api = _normalize_api(p.get("api"))
    if api:
        p["api"] = api

    # Clean field, county, district
    for key in ("field", "county", "district", "well_name", "well_type"):
        if key in p:
            p[key] = _clean_str(p[key])

    # Carry permit_id for cross-adapter consistency
    permit_no = _clean_str(p.get("permit_no"))
    if permit_no:
        p["permit_no"] = permit_no
        if "permit_id" not in p:
            p["permit_id"] = permit_no

    # Numeric fields: ensure they're numeric or None
    for key in ("depth_proposed", "td_reached", "ip_boed", "lateral_length_ft",
                "proppant_lbs", "frac_stages", "oil_bbl", "gas_mcf", "water_bbl"):
        val = p.get(key)
        if val is not None:
            try:
                p[key] = float(val) if isinstance(val, (int, float, str)) and str(val).strip() else None
            except (ValueError, TypeError):
                p[key] = None

    # Coordinates
    for key in ("latitude", "longitude"):
        val = p.get(key)
        if val is not None:
            try:
                p[key] = float(val)
            except (ValueError, TypeError):
                p[key] = None

    return p


def _build_source_event_id(obj: Mapping[str, Any], payload: Dict[str, Any]) -> str | None:
    """Derive a stable source_event_id from the raw object."""
    # Prefer explicit source_event_id from the JSONL row
    explicit = obj.get("source_event_id")
    if explicit and str(explicit).strip():
        return str(explicit).strip()
    # Fallback: api + type
    api = payload.get("api")
    etype = payload.get("type")
    if api and etype:
        return f"{api}|{etype}"
    return None


def _build_canonical_doc_id(source_event_id: str | None, payload: Dict[str, Any]) -> str:
    """Build a stable canonical_doc_id."""
    seed = f"{source_event_id or ''}|{payload.get('type', '')}|{payload.get('api', '')}"
    return f"{SOURCE_SYSTEM}:{sha256_hex(seed)[:16]}"


def iter_fixture_events(path: str = "sample_data/texas/rrc_raw_events.jsonl") -> Iterable[Dict[str, Any]]:
    """Yield raw event dicts from a JSONL fixture file."""
    p = Path(path)
    if not p.exists():
        return
    for line_no, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        yield obj


def ingest_fixture_to_db(path: str = "sample_data/texas/rrc_raw_events.jsonl") -> Tuple[int, int]:
    """
    Ingest TX RRC fixture events into event_log.
    Returns (inserted, processed).
    """
    inserted = 0
    processed = 0
    for obj in iter_fixture_events(path):
        processed += 1
        payload = obj.get("payload_json") or {}
        if not isinstance(payload, dict):
            payload = {}

        payload = _canonicalize_payload(payload)
        lineage_id = _normalize_lineage_id(payload)
        if lineage_id:
            payload["lineage_id"] = lineage_id

        event_time = _parse_dt(obj.get("event_time"))
        source_event_id = _build_source_event_id(obj, payload)

        raw_event = {
            "source_system": SOURCE_SYSTEM,
            "source_event_id": source_event_id,
            "event_time": event_time,
            "ingest_time": _now_utc(),
            "payload_json": payload,
            "content_hash": sha256_hex(json.dumps(payload, sort_keys=True, default=str)),
            "canonical_doc_id": _build_canonical_doc_id(source_event_id, payload),
        }
        did_insert, _id = insert_raw_event(raw_event)
        inserted += 1 if did_insert else 0
    return inserted, processed
