from ogree_alpha.entity_resolution import resolve_company


def test_resolve_by_exact_name():
    r = resolve_company(name="Frontier Rare Earths Ltd")
    assert r.company_id == "FRONTIER_REE"
    assert "FRE.V" in r.tickers
    assert r.method == "alias"
    assert r.confidence > 0.5


def test_resolve_by_alias():
    r = resolve_company(name="Permian Basin Resources")
    assert r.company_id == "PERMIAN_BASIN_RES"


def test_resolve_by_operator():
    r = resolve_company(operator="Eagle Ford Energy LLC")
    assert r.company_id == "EAGLE_FORD_ENERGY"


def test_resolve_unknown_returns_none():
    r = resolve_company(name="Totally Unknown Corp")
    assert r.company_id is None
    assert r.method == "none"
    assert r.confidence == 0.0


def test_resolve_none_inputs():
    r = resolve_company()
    assert r.method == "none"
