from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from ogree_alpha.universe import load_universe


@dataclass(frozen=True)
class ResolvedEntity:
    company_id: Optional[str]
    tickers: List[str]
    matched_name: Optional[str]
    confidence: float
    method: str  # "exact", "alias", "fallback", "none"


def _norm(s: str) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() or ch.isspace() else " " for ch in s).split())


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _build_alias_index(uni: Any) -> Dict[str, str]:
    """
    Returns: normalized_name -> company_id
    Uses:
      - companies[].name
      - companies[].aliases (optional)
      - companies[].tickers (optional; not used as name but can be added later)
    """
    idx: Dict[str, str] = {}
    companies = _get(uni, "companies", []) or []
    for c in companies:
        cid = _get(c, "company_id")
        if not cid:
            continue
        name = _get(c, "name")
        if isinstance(name, str) and name.strip():
            idx[_norm(name)] = str(cid)

        aliases = _get(c, "aliases", []) or []
        if isinstance(aliases, list):
            for a in aliases:
                if isinstance(a, str) and a.strip():
                    idx[_norm(a)] = str(cid)
    return idx


def _company_map(uni: Any) -> Dict[str, Any]:
    m: Dict[str, Any] = {}
    companies = _get(uni, "companies", []) or []
    for c in companies:
        cid = _get(c, "company_id")
        if cid:
            m[str(cid)] = c
    return m


def resolve_company(
    name: Optional[str] = None,
    operator: Optional[str] = None,
    *,
    universe: Any = None,
) -> ResolvedEntity:
    """
    Offline, deterministic resolution:
      1) exact/alias match against universe companies (name + aliases)
      2) fallback: if universe has exactly 1 company, return it
      3) none
    """
    uni = universe or load_universe()
    alias_idx = _build_alias_index(uni)
    companies = _company_map(uni)

    candidates: List[Tuple[str, str]] = []
    for raw in [name, operator]:
        if raw and raw.strip():
            n = _norm(raw)
            if n in alias_idx:
                candidates.append((alias_idx[n], raw))

    if candidates:
        company_id, matched_raw = candidates[0]
        c = companies.get(company_id)
        tickers_val = _get(c, "tickers", []) if c is not None else []
        tickers = list(tickers_val) if isinstance(tickers_val, list) else ([str(tickers_val)] if tickers_val else [])
        return ResolvedEntity(
            company_id=company_id,
            tickers=[str(t) for t in tickers],
            matched_name=matched_raw,
            confidence=0.95,
            method="alias",
        )

    # fallback: single-company universe
    if len(companies) == 1:
        only = next(iter(companies.values()))
        cid = str(_get(only, "company_id"))
        tickers_val = _get(only, "tickers", []) or []
        tickers = list(tickers_val) if isinstance(tickers_val, list) else [str(tickers_val)]
        return ResolvedEntity(
            company_id=cid,
            tickers=[str(t) for t in tickers],
            matched_name=None,
            confidence=0.25,
            method="fallback",
        )

    return ResolvedEntity(
        company_id=None,
        tickers=[],
        matched_name=None,
        confidence=0.0,
        method="none",
    )
