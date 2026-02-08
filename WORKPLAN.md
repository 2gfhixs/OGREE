# OGREE — Exploration Alpha (Track A) WORKPLAN

## North Star
Identify specific, actionable investment opportunities (small-cap “explosive optionality”) by maintaining a traceable line from **grant of exploration rights → exploration activity → exploration results**, combined with company health + macro/regime context. System emits intraday alerts and twice-daily reports; trades remain human-reviewed.

---

## Completed

### Phase 0 — Spec + Repo bootstrap
- Added full system spec (SPEC.md / SPEC_FULL.md) and established repo structure + packaging (pyproject.toml).

### Phase 1 — Core contracts + utilities
- Implemented minimal Pydantic v2 contracts (RawEvent, Alert, EvidencePointer, ScoreSummary, etc.).
- Added stable hashing utilities for canonical IDs and alert IDs.
- Added universe loader + `config/universe.yaml` (watchlist scaffolding).

### Phase 2 — Database foundation
- Added Postgres docker-compose, Alembic configuration, and initial migration creating `event_log` + `alerts` with required indexes (including partial unique index for `(source_system, source_event_id)` when `source_event_id IS NOT NULL`).
- Added SQLAlchemy models/session utilities and repo insert functions (idempotent conflict-safe inserts).
- Added DB setup steps to README.

### Phase 3 — Offline end-to-end demo
- Added demo pipeline: ingest JSONL → insert into DB → emit alerts/report.
- Added sample demo raw events (sample_data/raw_events.jsonl).
- Added tests proving offline E2E flow works and remains deterministic.

### Phase 4 — Alaska permits adapter (scaffold + live opt-in)
- Added Alaska permits adapter with fixture fallback and opt-in live fetch (env-gated).
- Implemented ingestion into `event_log` with normalized lineage/event payloads.
- Added smoke tests gated by env flags; stabilized parsing.

### Phase 5 — Multi-agent structure + deployment intent
- Documented intent to deploy on OpenClaw with multiple focused agents (AK, TX, REE/U, macro/regime, reporting), avoiding “one agent does everything”.
- Added early agent boundary decisions and data/ID flow notes.

### Phase 6 — Chain scoring + alerting + reporting
- Implemented chain progression scoring (permit/well evidence weighting) and alert generator.
- Implemented twice-daily report renderer (tier-grouped text + HTML output).
- Added DB-safe tests (JSON-safe payloads; idempotent inserts; stable IDs).

### Phase 7 — Opportunity ranking (bridge to “investment opportunities”)
- Added deterministic opportunity ranking output (Top Opportunities) and integrated into reporting surface.
- Stabilized formatting + tests to ensure consistent outputs.

### Phase 8 — Entity resolution (baseline restored)
- Introduced entity resolution hooks (resolve_company) and stabilized alert generator; tests green.
- Next step is to plumb resolved `company_id` into alerts cleanly and deterministically.

---

## Current Status (What runs today)
- Postgres + Alembic migrations apply cleanly; tables include `event_log` and `alerts`.
- Offline demo ingest + alert/report generation works.
- Alaska permits ingestion works with fixture fallback; AK chain scoring + reports are test-covered.
- Twice-daily report emits JSON containing `{subject, text, html}`.

---

## Next (Immediate)

### Phase 8.1 — Propagate resolved company identity into alerts
Goal: `resolve_company(operator=...)` affects alert payloads and downstream ranking/reporting.
- Update `build_alert(...)` to accept `company_id` and set it on the alert payload.
- Pass resolved `company_id` from `generate_and_insert_alerts`.
- Add/adjust one unit test asserting `company_id` propagation (keep IDs stable).

### Phase 9 — Texas RRC ingestion (stub first, then harden)
- Start with fixture-based adapter and normalized event schema (permit/completion/drilling result).
- Expect messy data & inconsistent formatting; budget extra time for parser/normalizer.

### Phase 10 — REE + Uranium ingestion (stub → richer evidence)
- Start with press releases + filings metadata extraction (avoid full NI 43-101 / S-K 1300 parsing initially).
- Add alias system for entity resolution (name/ticker changes; multiple listings TSX-V/OTC; shells).

---

## Known Pain Points / Risk Register
- Texas RRC data is notoriously messy (format drift, quirky API); parser agent needs extra budget.
- Junior miner entity resolution is hard (name/ticker changes, multi-listings TSX-V/OTC; shells); alias system must be robust.
- NI 43-101 / S-K 1300 PDF parsing is heterogeneous; best to stub initially with press release extraction + structured evidence pointers.

---

## Deployment Notes (OpenClaw)
- Prefer multiple specialized agents:
  - AK adapter agent
  - TX adapter agent
  - REE/U evidence agent
  - Entity resolution agent
  - Macro/regime agent (owned by global/macro team)
  - Reporting + alert delivery agent (2x/day email-ready output + intraday triggers)
- Keep core DB + IDs stable to support reliable backtests and incremental improvements.
