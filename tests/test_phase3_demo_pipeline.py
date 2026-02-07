import os

import pytest

from ogree_alpha.demo_pipeline import ingest_and_alert


def _count_jsonl_lines(path: str) -> int:
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_demo_pipeline_inserts_rows():
    path = "sample_data/raw_events.jsonl"
    out = ingest_and_alert(path)
    expected = _count_jsonl_lines(path)
    assert len(out) == expected
