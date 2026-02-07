from __future__ import annotations

import hashlib
import json
from typing import Any, Dict


def _normalize(obj: Any) -> Any:
    """
    Normalize JSON-like objects for stable hashing:
    - dict keys sorted
    - lists preserved in order
    """
    if isinstance(obj, dict):
        return {k: _normalize(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [_normalize(x) for x in obj]
    return obj


def stable_json_dumps(payload: Dict[str, Any]) -> str:
    """Deterministic JSON serialization for hashing."""
    normalized = _normalize(payload)
    return json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def content_hash(payload_json: Dict[str, Any]) -> str:
    """Hash for raw event payload content."""
    return sha256_hex(stable_json_dumps(payload_json))


def canonical_doc_id(source_system: str, content_hash_hex: str) -> str:
    """Stable document id derived from source + content hash."""
    return f"{source_system}:{content_hash_hex[:16]}"


def alert_id(canonical_doc_id: str, tier: str, event_type: str) -> str:
    """Stable alert id derived from canonical doc + tier + type."""
    base = f"{canonical_doc_id}|{tier}|{event_type}"
    return sha256_hex(base)[:24]
