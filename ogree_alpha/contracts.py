from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class Tier(StrEnum):
    low = "low"
    medium = "medium"
    high = "high"


class EventType(StrEnum):
    generic = "generic"


class EvidencePointer(BaseModel):
    model_config = ConfigDict(extra="allow")

    # flexible pointer to evidence: urls, filings, doc ids, etc.
    source: Optional[str] = None
    url: Optional[str] = None
    doc_id: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)


class ScoreSummary(BaseModel):
    model_config = ConfigDict(extra="allow")

    score: float = 0.0
    components: Dict[str, Any] = Field(default_factory=dict)
    rationale: Optional[str] = None


class RawEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_system: str
    source_event_id: Optional[str] = None
    event_time: Optional[datetime] = None
    ingest_time: Optional[datetime] = None

    payload_json: Dict[str, Any]
    content_hash: str
    canonical_doc_id: Optional[str] = None


class Alert(BaseModel):
    model_config = ConfigDict(extra="allow")

    alert_id: str
    tier: str
    event_type: str

    event_time: Optional[datetime] = None
    ingest_time: Optional[datetime] = None

    company_id: Optional[str] = None
    asset_id: Optional[str] = None

    canonical_doc_id: str
    evidence_pointer: Dict[str, Any]
    score_summary: Dict[str, Any]

    summary: str
    details: Dict[str, Any]
    regime_context: Optional[Dict[str, Any]] = None
