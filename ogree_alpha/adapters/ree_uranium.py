from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from ogree_alpha.hashing import sha256_hex
from ogree_alpha.db.repo import insert_raw_event


SOURCE_SYSTEM = "ree_uranium"

# Canonical event types for the REE/U lifecycle
VALID_TYPES = {
    "claims_staked",
    "exploration_permit",
    "drill_assay",
    "resource_estimate",
    "pea_published",
    "pfs_published",
    "feasibility_study",
    "financing_closed",
    "financing_announced",
    "offtake_agreement",
    "policy_designation",
    "production_decision",
    "construction_start",
    "plugging_report",
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
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
            try:
                return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = " ".join(value.strip().split())
    return value if value else None


def _normalize_commodity(raw: Any) -> str | None:
    if not raw:
        return None
    c = str(raw).strip().lower()
    aliases = {
        "ree": "REE",
        "rare earths": "REE",
        "rare earth": "REE",
        "rare earth elements": "REE",
        "uranium": "uranium",
        "u3o8": "uranium",
        "u": "uranium",
    }
    return aliases.get(c, c)


def _normalize_type(raw_type: Any) -> str:
    if not raw_type or not isinstance(raw_type, str):
        return "unknown"
    key = raw_type.strip().lower().replace(" ", "_").replace("-", "_")
    return key if key in VALID_TYPES else key


def _derive_lineage_id(payload: Dict[str, Any]) -> str | None:
    """
    REE/U lineage is project-based: company + project name.
    All events for the same project group together for chain scoring.
    """
    company = _clean_str(payload.get("company"))
    project = _clean_str(payload.get("project"))
    if company and project:
        return sha256_hex(f"REE_U|{company}|{project}")[:20]
    # Policy events don't have a project — use policy + commodity
    if payload.get("type") == "policy_designation":
        policy = _clean_str(payload.get("policy")) or "unknown"
        commodity = payload.get("commodity") or "unknown"
        return sha256_hex(f"REE_U|policy|{policy}|{commodity}")[:20]
    return None


def _canonicalize_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    p = dict(payload) if isinstance(payload, dict) else {}

    p["type"] = _normalize_type(p.get("type"))
    p["commodity"] = _normalize_commodity(p.get("commodity"))
    p["company"] = _clean_str(p.get("company"))
    p["project"] = _clean_str(p.get("project"))
    p["region"] = _clean_str(p.get("region"))
    p["jurisdiction"] = _clean_str(p.get("jurisdiction"))

    # Normalize tickers to list of strings
    tickers = p.get("tickers")
    if isinstance(tickers, list):
        p["tickers"] = [str(t).strip() for t in tickers if str(t).strip()]
    elif isinstance(tickers, str) and tickers.strip():
        p["tickers"] = [t.strip() for t in tickers.split(",") if t.strip()]
    else:
        p["tickers"] = []

    # Numeric fields
    for key in ("treo_pct", "mreo_pct", "u3o8_ppm", "gt_metric",
                "interval_m", "interval_ft", "from_m", "to_m", "from_ft", "to_ft",
                "tonnage_mt", "grade_treo_pct", "grade_u3o8_pct",
                "contained_treo_kt", "contained_u3o8_mlbs",
                "npv_8_musd", "irr_pct", "capex_musd", "opex_per_kg_reo",
                "payback_years", "amount_cad", "price_per_share_cad",
                "shares_issued", "quantity_mlbs",
                "claims_count", "area_ha", "area_acres"):
        val = p.get(key)
        if val is not None:
            try:
                p[key] = float(val)
            except (ValueError, TypeError):
                p[key] = None

    return p


def _build_source_event_id(obj: Mapping[str, Any], payload: Dict[str, Any]) -> str | None:
    explicit = obj.get("source_event_id")
    if explicit and str(explicit).strip():
        return str(explicit).strip()
    # Fallback: company + type + project
    parts = [payload.get("company") or "", payload.get("type") or "", payload.get("project") or ""]
    seed = "|".join(parts)
    return sha256_hex(seed)[:16] if any(parts) else None


def _build_canonical_doc_id(source_event_id: str | None, payload: Dict[str, Any]) -> str:
    seed = f"{source_event_id or ''}|{payload.get('type', '')}|{payload.get('company', '')}|{payload.get('project', '')}"
    return f"{SOURCE_SYSTEM}:{sha256_hex(seed)[:16]}"


def iter_fixture_events(path: str = "sample_data/ree_uranium/events.jsonl") -> Iterable[Dict[str, Any]]:
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


def ingest_fixture_to_db(path: str = "sample_data/ree_uranium/events.jsonl") -> Tuple[int, int]:
    """
    Ingest REE/Uranium fixture events into event_log.
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
        lineage_id = _derive_lineage_id(payload)
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
