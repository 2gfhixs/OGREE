from ogree_alpha.entity_resolution import resolve_company


def test_resolve_company_fallback_single_company():
    # Universe currently contains one placeholder company; we should at least resolve deterministically.
    r = resolve_company(name="Anything")
    assert r.company_id is not None
    assert isinstance(r.tickers, list)
    assert r.method in ("fallback", "alias", "exact")
