from ogree_alpha.entity_resolution import resolve_company


def test_resolve_by_exact_name():
    r = resolve_company(name="Ucore Rare Metals Inc.")
    assert r.company_id == "UCORE_RARE_METALS"
    assert "UCU.V" in r.tickers
    assert r.method == "alias"
    assert r.confidence > 0.5


def test_resolve_by_alias():
    r = resolve_company(name="Permian Resources Corp")
    assert r.company_id == "PERMIAN_RESOURCES"


def test_resolve_by_operator():
    r = resolve_company(operator="Comstock Resources, Inc.")
    assert r.company_id == "COMSTOCK_RESOURCES"


def test_resolve_unknown_returns_none():
    r = resolve_company(name="Totally Unknown Corp")
    assert r.company_id is None
    assert r.method == "none"
    assert r.confidence == 0.0


def test_resolve_none_inputs():
    r = resolve_company()
    assert r.method == "none"
