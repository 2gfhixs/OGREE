import os
import pytest

from ogree_alpha.adapters.texas_rrc import ingest_fixture_to_db


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_tx_rrc_fixture_ingest_inserts_at_least_one():
    inserted, processed = ingest_fixture_to_db()
    assert processed >= 1
