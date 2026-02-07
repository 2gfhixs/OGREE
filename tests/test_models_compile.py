def test_imports_compile():
    # Core package import
    import ogree_alpha  # noqa: F401

    # Contracts + hashing + universe
    from ogree_alpha.contracts import Alert, RawEvent  # noqa: F401
    from ogree_alpha.hashing import content_hash, canonical_doc_id, alert_id  # noqa: F401
    from ogree_alpha.universe import load_universe  # noqa: F401

    # DB layer imports
    from ogree_alpha.db.models import Alert as AlertModel, EventLog  # noqa: F401
    from ogree_alpha.db.session import get_session  # noqa: F401
    from ogree_alpha.db.repo import insert_alert, insert_raw_event  # noqa: F401

    assert Alert is not None and RawEvent is not None
    assert AlertModel is not None and EventLog is not None
    assert callable(load_universe)
    assert callable(get_session)
    assert callable(insert_raw_event)
    assert callable(insert_alert)
