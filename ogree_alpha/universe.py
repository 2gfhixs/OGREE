from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class Universe:
    version: int
    assets: List[Dict[str, Any]]
    companies: List[Dict[str, Any]]
    watchlists: List[Dict[str, Any]]


def load_universe(path: str = "config/universe.yaml") -> Universe:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    version = int(data.get("version", 1))
    assets = list(data.get("assets", []) or [])
    companies = list(data.get("companies", []) or [])
    watchlists = list(data.get("watchlists", []) or [])

    return Universe(version=version, assets=assets, companies=companies, watchlists=watchlists)


def get_watchlist(universe: Universe, name: str = "default") -> Optional[Dict[str, Any]]:
    for wl in universe.watchlists:
        if wl.get("name") == name:
            return wl
    return None
