from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Tuple

from ogree_alpha.db.repo import insert_raw_event
from ogree_alpha.entity_resolution import resolve_company
from ogree_alpha.hashing import sha256_hex


SOURCE_SYSTEM = "sec_edgar"

VALID_TYPES = {
    "insider_buy",
    "insider_sell",
    "insider_option_exercise",
    "institutional_13g",
    "institutional_13f",
}

_TYPE_ALIASES: Dict[str, str] = {
    "insider_buy": "insider_buy",
    "insider_purchase": "insider_buy",
    "purchase": "insider_buy",
    "buy": "insider_buy",
    "open_market_purchase": "insider_buy",
    "insider_sell": "insider_sell",
    "insider_sale": "insider_sell",
    "sale": "insider_sell",
    "sell": "insider_sell",
    "insider_option_exercise": "insider_option_exercise",
    "option_exercise": "insider_option_exercise",
    "exercise": "insider_option_exercise",
    "institutional_13g": "institutional_13g",
    "13g": "institutional_13g",
    "schedule_13g": "institutional_13g",
    "institutional_13f": "institutional_13f",
    "13f": "institutional_13f",
    "form_13f": "institutional_13f",
}

_TX_TYPE_ALIASES: Dict[str, str] = {
    "purchase": "purchase",
    "buy": "purchase",
    "open_market_purchase": "purchase",
    "acquired": "purchase",
    "sale": "sale",
    "sell": "sale",
    "disposed": "sale",
    "exercise": "exercise",
    "option_exercise": "exercise",
    "derivative_exercise": "exercise",
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
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
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


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _norm_key(value: Any) -> str:
    s = _clean_str(value) or ""
    return s.lower().replace("-", "_").replace(" ", "_")


def _norm_name(value: Any) -> str:
    raw = _clean_str(value) or ""
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in raw)
    return " ".join(cleaned.split())


def _normalize_type(raw_type: Any) -> str:
    key = _norm_key(raw_type)
    if not key:
        return "unknown"
    normalized = _TYPE_ALIASES.get(key, key)
    return normalized if normalized in VALID_TYPES else normalized


def _normalize_relationship(raw: Any) -> str | None:
    rel = (_clean_str(raw) or "").lower()
    if not rel:
        return None
    if "10%" in rel or "10 percent" in rel or "beneficial owner" in rel:
        return "10% owner"
    if "director" in rel:
        return "director"
    if "officer" in rel or any(k in rel for k in ("ceo", "cfo", "coo", "president", "vp", "chief")):
        return "officer"
    if any(k in rel for k in ("institution", "fund", "adviser", "advisor", "asset management")):
        return "institution"
    return rel


def _normalize_transaction_type(raw: Any, *, normalized_event_type: str) -> str | None:
    key = _norm_key(raw)
    if key:
        return _TX_TYPE_ALIASES.get(key, key)
    if normalized_event_type == "insider_buy":
        return "purchase"
    if normalized_event_type == "insider_sell":
        return "sale"
    if normalized_event_type == "insider_option_exercise":
        return "exercise"
    return None


def _normalize_tickers(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()]
    if isinstance(raw, str):
        parts = [x.strip() for x in raw.split(",")]
        return [p for p in parts if p]
    return []


def _derive_lineage_id(payload: Mapping[str, Any]) -> str | None:
    company_id = _clean_str(payload.get("company_id"))
    if company_id:
        return f"SEC:{company_id}"
    company = _norm_name(payload.get("company"))
    if company:
        return f"SEC:{sha256_hex(company)[:16]}"
    return None


def _canonicalize_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    p = dict(payload) if isinstance(payload, dict) else {}

    p["type"] = _normalize_type(p.get("type"))
    p["filer_name"] = _clean_str(p.get("filer_name"))
    p["relationship"] = _normalize_relationship(p.get("relationship"))
    p["transaction_type"] = _normalize_transaction_type(
        p.get("transaction_type"), normalized_event_type=p["type"]
    )
    p["shares"] = _as_float(p.get("shares"))
    p["price_per_share"] = _as_float(p.get("price_per_share"))
    total_value = _as_float(p.get("total_value"))
    if total_value is None and p["shares"] is not None and p["price_per_share"] is not None:
        total_value = round(p["shares"] * p["price_per_share"], 2)
    p["total_value"] = total_value

    p["company"] = _clean_str(p.get("company") or p.get("issuer_name"))
    p["tickers"] = _normalize_tickers(p.get("tickers"))
    p["form_type"] = _clean_str(p.get("form_type"))
    p["filing_accession"] = _clean_str(p.get("filing_accession") or p.get("accession_no"))
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


def _build_source_event_id(
    obj: Mapping[str, Any],
    payload: Mapping[str, Any],
    event_time: datetime | None,
) -> str:
    explicit = _clean_str(obj.get("source_event_id"))
    if explicit:
        return explicit

    seed = "|".join(
        [
            _clean_str(payload.get("filing_accession")) or "",
            _clean_str(payload.get("type")) or "",
            _clean_str(payload.get("filer_name")) or "",
            _clean_str(payload.get("company")) or "",
            _clean_str(payload.get("transaction_type")) or "",
            str(payload.get("shares") or ""),
            str(event_time.isoformat() if isinstance(event_time, datetime) else ""),
        ]
    )
    if seed.strip("|"):
        return f"sec_{sha256_hex(seed)[:24]}"
    return f"sec_{sha256_hex(json.dumps(dict(payload), sort_keys=True, default=str))[:24]}"


def _build_canonical_doc_id(source_event_id: str, payload: Mapping[str, Any]) -> str:
    seed = "|".join(
        [
            source_event_id,
            _clean_str(payload.get("type")) or "",
            _clean_str(payload.get("company")) or "",
            _clean_str(payload.get("filer_name")) or "",
        ]
    )
    return f"{SOURCE_SYSTEM}:{sha256_hex(seed)[:16]}"


def iter_fixture_events(path: str = "sample_data/sec_edgar/form4_events.jsonl") -> Iterable[Dict[str, Any]]:
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


def ingest_fixture_to_db(path: str = "sample_data/sec_edgar/form4_events.jsonl") -> Tuple[int, int]:
    inserted = 0
    processed = 0

    for obj in iter_fixture_events(path):
        processed += 1
        payload = obj.get("payload_json") or {}
        if not isinstance(payload, dict):
            payload = {}
        payload = _canonicalize_payload(payload)

        event_time = _parse_dt(obj.get("event_time"))
        source_event_id = _build_source_event_id(obj, payload, event_time)

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
