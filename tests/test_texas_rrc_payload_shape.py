import os
import pytest

from ogree_alpha.adapters.texas_rrc import iter_fixture_events, _normalize_lineage_id


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_tx_payload_has_chain_keys():
    # Validate fixture shape + lineage derivation logic
    events = list(iter_fixture_events())
    assert len(events) >= 1

    for e in events:
        payload = e.get("payload_json") or {}
        assert isinstance(payload, dict)
        assert "type" in payload
        assert (payload.get("region") in (None, "Texas"))  # canonicalize sets it
        lineage = _normalize_lineage_id(payload)
        assert lineage is not None and len(lineage) > 0
