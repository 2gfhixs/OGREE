import os

import pytest
from sqlalchemy import text

from ogree_alpha.db.session import get_session


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_db_connectivity():
    with get_session() as session:
        assert session.execute(text("SELECT 1")).scalar_one() == 1
