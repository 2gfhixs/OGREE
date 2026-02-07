import os

import pytest
from sqlalchemy import text

from ogree_alpha.demo_pipeline import ingest_and_alert
from ogree_alpha.db.session import get_session


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_demo_pipeline_inserts_rows():
    # run demo ingest (idempotent-ish; may add rows if DB is fresh)
    out = ingest_and_alert("sample_data/raw_events.jsonl")
    assert len(out) == 3

    with get_session() as session:
        ev = session.execute(text("select count(*) from event_log")).scalar_one()
        al = session.execute(text("select count(*) from alerts")).scalar_one()

    # counts should be >= 3 even if test re-runs
    assert int(ev) >= 3
    assert int(al) >= 3
