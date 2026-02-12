from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Set, Tuple

# Category map
# A: Permit/claims activity
# B: Drill results/assays/well completions
# C: Resource estimates/PEA/PFS
# D: Financing/offtake/deal
# E: Insider buying/institutional accumulation
# F: Policy/macro tailwind
_CATEGORY_A_TYPES = {
    "lease_grant",
    "permit_filed",
    "permit_issued",
    "drilling_permit",
    "claims_staked",
    "exploration_permit",
}
_CATEGORY_B_TYPES = {
    "drill_result",
    "drill_assay",
    "completion_reported",
    "well_completion",
    "well_record",
}
_CATEGORY_C_TYPES = {
    "resource_estimate",
    "pea_published",
    "pfs_published",
    "feasibility_study",
}
_CATEGORY_D_TYPES = {
    "financing_closed",
    "financing_announced",
    "offtake_agreement",
}
_CATEGORY_E_TYPES = {
    "insider_buy",
    "institutional_13g",
    "institutional_13f",
}
_CATEGORY_F_TYPES = {
    "policy_designation",
    "policy_final_rule",
    "policy_nprm_open",
    "policy_comment_deadline",
    "congressional_trade_disclosure",
    "legislation_committee_advance",
}


def _coerce_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _norm_name(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in raw)
    out = " ".join(cleaned.split())
    return out if out else None


def _company_keys(company_id: Any = None, company: Any = None, operator: Any = None) -> List[str]:
    keys: List[str] = []
    if company_id and str(company_id).strip():
        keys.append(f"company_id:{str(company_id).strip()}")
    name = _norm_name(company) or _norm_name(operator)
    if name:
        keys.append(f"company_name:{name}")
    return keys


def _event_keys(payload: Mapping[str, Any]) -> List[str]:
    keys: List[str] = []
    lineage_id = payload.get("lineage_id")
    if lineage_id and str(lineage_id).strip():
        keys.append(f"lineage:{str(lineage_id).strip()}")
    company_keys = _company_keys(
        company_id=payload.get("company_id"),
        company=payload.get("company"),
        operator=payload.get("operator"),
    )
    keys.extend(company_keys)
    return keys


def _row_keys(row: Mapping[str, Any]) -> List[str]:
    keys: List[str] = []
    lineage_id = row.get("lineage_id")
    if lineage_id and str(lineage_id).strip():
        keys.append(f"lineage:{str(lineage_id).strip()}")
    company_keys = _company_keys(
        company_id=row.get("company_id"),
        company=row.get("company"),
        operator=row.get("operator"),
    )
    keys.extend(company_keys)
    return keys


def _event_categories(payload: Mapping[str, Any]) -> Set[str]:
    t = str(payload.get("type") or "").strip().lower()
    if not t:
        return set()
    categories: Set[str] = set()
    if t in _CATEGORY_A_TYPES:
        categories.add("A")
    if t in _CATEGORY_B_TYPES:
        categories.add("B")
    if t in _CATEGORY_C_TYPES:
        categories.add("C")
    if t in _CATEGORY_D_TYPES:
        categories.add("D")
    if t in _CATEGORY_E_TYPES:
        categories.add("E")
    if t in _CATEGORY_F_TYPES or any(k in t for k in ("policy", "macro", "rule", "nprm", "congress", "legislation", "committee")):
        categories.add("F")
    return categories


def _event_time(event: Mapping[str, Any]) -> datetime | None:
    return _coerce_utc(event.get("event_time") or event.get("ingest_time"))


def _build_signal_index(
    events: Sequence[Mapping[str, Any]],
) -> Tuple[Dict[str, List[Tuple[datetime, str]]], Dict[str, datetime]]:
    signals_by_key: Dict[str, List[Tuple[datetime, str]]] = defaultdict(list)
    latest_by_key: Dict[str, datetime] = {}

    for event in events:
        payload = event.get("payload_json") or {}
        if not isinstance(payload, Mapping):
            continue
        dt = _event_time(event)
        if dt is None:
            continue
        cats = _event_categories(payload)
        if not cats:
            continue

        keys = _event_keys(payload)
        if not keys:
            continue
        for key in keys:
            for category in cats:
                signals_by_key[key].append((dt, category))
            if key not in latest_by_key or dt > latest_by_key[key]:
                latest_by_key[key] = dt

    return signals_by_key, latest_by_key


def _categories_within_window(
    signal_points: Iterable[Tuple[datetime, str]],
    *,
    window_start: datetime,
    window_end: datetime,
) -> Set[str]:
    categories: Set[str] = set()
    for dt, category in signal_points:
        if window_start <= dt <= window_end:
            categories.add(category)
    return categories


def apply_convergence(
    rows: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    *,
    window_days: int = 30,
) -> List[Dict[str, Any]]:
    """
    Enrich chain score rows with convergence metadata:
      - convergence_score: count of distinct categories in window
      - convergence_categories: sorted list of category labels ["A", ...]
    """
    signals_by_key, latest_by_key = _build_signal_index(events)
    out: List[Dict[str, Any]] = []
    window = timedelta(days=window_days)

    for row in rows:
        row_mut = dict(row)
        keys = _row_keys(row_mut)
        row_anchor = _coerce_utc(row_mut.get("last_event_time"))
        candidate_times = [latest_by_key.get(k) for k in keys if latest_by_key.get(k) is not None]
        if candidate_times:
            if row_anchor is not None:
                anchor = max([row_anchor, *candidate_times])
            else:
                anchor = max(candidate_times)
        else:
            anchor = row_anchor

        if anchor is None:
            row_mut["convergence_score"] = 0
            row_mut["convergence_categories"] = []
            out.append(row_mut)
            continue

        window_start = anchor - window
        categories: Set[str] = set()
        for key in keys:
            categories |= _categories_within_window(
                signals_by_key.get(key, []),
                window_start=window_start,
                window_end=anchor,
            )

        cats_sorted = sorted(categories)
        row_mut["convergence_score"] = len(cats_sorted)
        row_mut["convergence_categories"] = cats_sorted
        out.append(row_mut)

    return out
