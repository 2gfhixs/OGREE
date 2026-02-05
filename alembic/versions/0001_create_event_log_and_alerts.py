"""create event_log and alerts tables

Revision ID: 0001_create_event_log_and_alerts
Revises:
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_create_event_log_and_alerts"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "event_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_system", sa.Text(), nullable=False),
        sa.Column("source_event_id", sa.Text(), nullable=True),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "ingest_time",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("canonical_doc_id", sa.Text(), nullable=True),
    )
    op.create_index("ix_event_log_event_time", "event_log", ["event_time"])
    op.create_index("ix_event_log_ingest_time", "event_log", ["ingest_time"])
    op.create_index(
        "uq_event_log_source_system_event_id",
        "event_log",
        ["source_system", "source_event_id"],
        unique=True,
        postgresql_where=sa.text("source_event_id IS NOT NULL"),
    )

    op.create_table(
        "alerts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("alert_id", sa.Text(), nullable=False, unique=True),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingest_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("company_id", sa.Text(), nullable=True),
        sa.Column("asset_id", sa.Text(), nullable=True),
        sa.Column("canonical_doc_id", sa.Text(), nullable=False),
        sa.Column(
            "evidence_pointer",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "score_summary",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "regime_context",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_table("alerts")
    op.drop_index("uq_event_log_source_system_event_id", table_name="event_log")
    op.drop_index("ix_event_log_ingest_time", table_name="event_log")
    op.drop_index("ix_event_log_event_time", table_name="event_log")
    op.drop_table("event_log")
