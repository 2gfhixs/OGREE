"""Repository functions."""

from __future__ import annotations

from typing import Any, Mapping, Tuple

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from ogree_alpha.db.models import Alert, EventLog
from ogree_alpha.db.session import get_session


def insert_raw_event(raw_event: Mapping[str, Any]) -> Tuple[bool, int]:
    """
    Insert into event_log idempotently.

    Returns (inserted, id). Uses partial unique index semantics:
    (source_system, source_event_id) where source_event_id is not null.
    """
    with get_session() as session:
        stmt = (
            insert(EventLog)
            .values(**raw_event)
            .on_conflict_do_nothing(
                index_elements=[EventLog.source_system, EventLog.source_event_id],
                index_where=EventLog.source_event_id.is_not(None),
            )
            .returning(EventLog.id)
        )
        row = session.execute(stmt).first()
        if row is not None:
            session.commit()
            return True, int(row[0])

        # Conflict case: fetch existing row id (requires source_event_id present)
        existing_id = session.execute(
            select(EventLog.id).where(
                EventLog.source_system == raw_event["source_system"],
                EventLog.source_event_id == raw_event["source_event_id"],
            )
        ).scalar_one()
        session.commit()
        return False, int(existing_id)


def insert_alert(alert: Mapping[str, Any]) -> bool:
    """Insert into alerts idempotently (unique on alert_id)."""
    with get_session() as session:
        stmt = (
            insert(Alert)
            .values(**alert)
            .on_conflict_do_nothing(index_elements=[Alert.alert_id])
        )
        result = session.execute(stmt)
        session.commit()
        return (result.rowcount or 0) > 0
