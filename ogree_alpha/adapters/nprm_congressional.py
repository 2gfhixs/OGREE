from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Tuple

from ogree_alpha.db.repo import insert_raw_event
from ogree_alpha.entity_resolution import resolve_company
from ogree_alpha.hashing import sha256_hex


SOURCE_SYSTEM = "nprm_congressional"

VALID_TYPES = {
    "policy_nprm_open",
    "policy_comment_deadline",
    "congressional_trade_disclosure",
    "legislation_committee_advance",
}
_TYPE_ALIASES: Dict[str, str] = {
    "policy_nprm_open": "policy_nprm_open",
    "nprm_open": "policy_nprm_open",
    "nprm": "policy_nprm_open",
    "policy_comment_deadline": "policy_comment_deadline",
    "comment_deadline": "policy_comment_deadline",
    "public_comment_deadline": "policy_comment_deadline",
    "congressional_trade_disclosure": "congressional_trade_disclosure",
    "congressional_trade": "congressional_trade_disclosure",
    "house_trade_disclosure": "congressional_trade_disclosure",
    "legislation_committee_advance": "legislation_committee_advance",
    "committee_advance": "legislation_committee_advance",
    "bill_committee_advance": "legislation_committee_advance",
}
_IMPACT_ALIASES: Dict[str, str] = {
    "favorable": "favorable",
    "positive": "favorable",
    "bullish": "favorable",
    "adverse": "adverse",
    "negative": "adverse",
    "bearish": "adverse",
    "neutral": "neutral",
    "mixed": "mixed",
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = " ".join(value.strip().split())
    return value if value else None


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
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _normalize_type(value: Any) -> str:
    key = (_clean_str(value) or "").lower().replace("-", "_").replace(" ", "_")
    if not key:
        return "unknown"
    normalized = _TYPE_ALIASES.get(key, key)
    return normalized if normalized in VALID_TYPES else normalized


def _normalize_impact(value: Any) -> str | None:
    key = (_clean_str(value) or "").lower()
    if not key:
        return None
    return _IMPACT_ALIASES.get(key, key)


def _normalize_tickers(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()]
    if isinstance(raw, str):
        return [t.strip() for t in raw.split(",") if t.strip()]
    return []


def _derive_lineage_id(payload: Mapping[str, Any]) -> str | None:
    company_id = _clean_str(payload.get("company_id"))
    if company_id:
        return f"POLICY:{company_id}"
    company = _clean_str(payload.get("company"))
    if company:
        return f"POLICY:{sha256_hex(company.lower())[:16]}"
    bill_id = _clean_str(payload.get("bill_id"))
    if bill_id:
        return f"POLICY:{sha256_hex(bill_id.lower())[:16]}"
    docket_id = _clean_str(payload.get("docket_id"))
    if docket_id:
        return f"POLICY:{sha256_hex(docket_id.lower())[:16]}"
    return None


def _canonicalize_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    p = dict(payload) if isinstance(payload, dict) else {}
    p["type"] = _normalize_type(p.get("type"))
    p["title"] = _clean_str(p.get("title"))
    p["agency"] = _clean_str(p.get("agency"))
    p["docket_id"] = _clean_str(p.get("docket_id"))
    p["bill_id"] = _clean_str(p.get("bill_id"))
    p["committee"] = _clean_str(p.get("committee"))
    p["legislator"] = _clean_str(p.get("legislator"))
    p["trade_action"] = _clean_str(p.get("trade_action"))
    p["impact_direction"] = _normalize_impact(p.get("impact_direction"))
    p["impact_summary"] = _clean_str(p.get("impact_summary"))
    p["comment_deadline"] = _clean_str(p.get("comment_deadline"))
    p["company"] = _clean_str(p.get("company"))
    p["tickers"] = _normalize_tickers(p.get("tickers"))
    p["source_url"] = _clean_str(p.get("source_url"))
    p["region"] = _clean_str(p.get("region")) or "US"

    if p["company"]:
        resolved = resolve_company(name=p["company"])
        if resolved.company_id:
            p["company_id"] = resolved.company_id
            if not p["tickers"] and resolved.tickers:
                p["tickers"] = [str(t) for t in resolved.tickers]

    lineage_id = _derive_lineage_id(p)
    if lineage_id:
        p["lineage_id"] = lineage_id
    return p


def _build_source_event_id(obj: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    explicit = _clean_str(obj.get("source_event_id"))
    if explicit:
        return explicit
    parts = [
        _clean_str(payload.get("type")) or "",
        _clean_str(payload.get("docket_id")) or "",
        _clean_str(payload.get("bill_id")) or "",
        _clean_str(payload.get("company")) or "",
        _clean_str(payload.get("legislator")) or "",
    ]
    seed = "|".join(parts)
    if seed.strip("|"):
        return f"pol_{sha256_hex(seed)[:24]}"
    return f"pol_{sha256_hex(json.dumps(dict(payload), sort_keys=True, default=str))[:24]}"


def _build_canonical_doc_id(source_event_id: str, payload: Mapping[str, Any]) -> str:
    seed = "|".join(
        [
            source_event_id,
            _clean_str(payload.get("type")) or "",
            _clean_str(payload.get("bill_id")) or "",
            _clean_str(payload.get("docket_id")) or "",
        ]
    )
    return f"{SOURCE_SYSTEM}:{sha256_hex(seed)[:16]}"


def iter_fixture_events(path: str = "sample_data/policy_signals/events.jsonl") -> Iterable[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


def ingest_fixture_to_db(path: str = "sample_data/policy_signals/events.jsonl") -> Tuple[int, int]:
    inserted = 0
    processed = 0
    for obj in iter_fixture_events(path):
        processed += 1
        payload = obj.get("payload_json") or {}
        if not isinstance(payload, dict):
            payload = {}
        payload = _canonicalize_payload(payload)
        event_time = (
            _parse_dt(obj.get("event_time"))
            or _parse_dt(payload.get("comment_deadline"))
        )
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
