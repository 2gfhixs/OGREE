# OGREE — Exploration Alpha (Track A) System Spec

## 0) Purpose

Build an event-driven research system (“Exploration Alpha”) that identifies **explosive optionality** in small-cap natural resource companies using **traceable evidence** from **grant of exploration rights → exploration activity → exploration results**, combined with company health signals and a macro “regime engine” snapshot. The system produces **intraday alerts** and **twice-daily email-ready reports**. Trades are **human reviewed**; system does not autonomously execute trades in production by default.

Primary interest verticals for Track A:
1) **Onshore natural gas** — Alaska + Texas  
2) **REE + Uranium** (include uranium explicitly)  
Secondary / future: water and other natural resources.

Out of scope: anything that facilitates illegal trading (nonpublic info, privileged access). System consumes public data, public market data, and public media (news/social) and produces research signals.

---

## 1) Track A: Exploration Alpha (Bottom-Up)

### 1.1 Goal
Identify “explosive optionality” in small caps with measurable catalysts by building:
- **Entity graph**: `asset → company → catalysts → evidence`
- **Rights line**: `rights grant → permits/operations → results → probability update`
- **Catalyst calendar** + scoring model
- **Intraday triggers** + alerts for human review

### 1.2 Scope constraints
- Universe size is manageable (a few hundred entities at a time).
- Model will be **backtested extensively** before any automation beyond alerts.
- Outputs are not monetized; internal use; human review.

---

## 2) Data Sources & Ingestion Targets (MVP)

### 2.1 Rights / leases / permits / activity (gas)
**Texas (onshore gas)**:
- Texas Railroad Commission (RRC) permits, completions, wells, filings (MVP via CSV/HTML in /samples, later via official endpoints/scrapes).

**Alaska (onshore gas / AK plays)**:
- Alaska DNR leases and/or AOGCC well activity/production (MVP via /samples, later via official sources).

### 2.2 REE + Uranium (exploration + financings)
- Company press releases, regulatory filings, technical reports (NI 43-101 / S-K 1300 where relevant), drill results (assays), permitting milestones, financings.

### 2.3 Company health
- Earnings, balance sheet metrics, cash burn/runway, debt maturity, dilution/financing, royalties/streams, hedges (if applicable), and other survivability signals.

### 2.4 Macro/regime + geopolitics
- “Regime engine” snapshot: a structured environment state representing risk regime, commodity cycle factors, permitting friction, geopolitical risk, etc.
- Optional feeds: Polymarket, X (Twitter), and curated news sources (public). MVP can stub these and add later.

---

## 3) Core Requirements: Determinism, Traceability, Idempotency

### 3.1 Deterministic runs
Given the same `/samples/*` inputs and fixed config, the pipeline must:
- produce **byte-identical** `/out/alerts.jsonl` on repeated runs
- produce stable IDs (doc IDs, alert IDs, entity IDs)
- avoid nondeterministic ordering (always sort by event_time then ingest_time then stable IDs)

### 3.2 Idempotent writes
- **Raw events** are append-only, but must dedupe when `source_event_id` exists:
  - uniqueness key: `(source_system, source_event_id)` **where source_event_id is not null**
- **Evidence** upserts by `canonical_doc_id`
- **Alerts** dedupe by `alert_id`
- Email/report runs track `last_sent_at` + `last_digest_hash` to avoid repeats

### 3.3 Concurrency safety
Repository inserts must be safe under concurrency:
- Use Postgres `ON CONFLICT DO NOTHING` / conflict-aware inserts for raw events and alerts.
- Avoid “check-then-insert” races unless backed by DB constraints.

---

## 4) Entity Model & Graph

### 4.1 Entities (minimum)
- **Company**: ticker(s), name, exchange, aliases, vertical tags, jurisdiction(s)
- **Asset/Project**: basin/region, commodity tags (gas/REE/U), geometry (optional)
- **Rights**: lease, mineral rights, claims, permits; dates; jurisdiction; status
- **Catalyst**: event window, type, probability/impact scoring, evidence pointers
- **Evidence**: pointer to source, extracted facts, confidence, event_time & ingest_time
- **EnvironmentSnapshot**: regime state + factor surface

### 4.2 Graph edges (minimum)
- company ↔ asset: owns/operates/options, interest %, operator flag
- asset ↔ rights: rights attached to asset
- company/asset ↔ evidence: evidence references with extracted facts
- company/asset ↔ catalysts: inferred or explicit upcoming events

---

## 5) Pipeline Architecture (Agents)

Each stage is an “agent” with a narrow contract and clear IO. The system can run each stage independently or end-to-end.

### 5.1 Agents
1) **CollectorAgent**
   - Input: source definitions
   - Output: RawEvents (append-only)
2) **ParserAgent**
   - Input: RawEvents
   - Output: Evidence objects + extracted_facts
3) **ResolverAgent**
   - Input: Evidence
   - Output: entity linking to company_id and asset_id (via universe + aliases)
4) **ScorerAgent**
   - Input: linked Evidence + prior company_state/asset_state
   - Output: MicroScore + health/runway metrics + flags + explanations
5) **RegimeAdapterAgent**
   - Input: EnvironmentSnapshot
   - Output: AdjustedScore modifiers and regime context per vertical/exposure
6) **AlertAgent**
   - Input: Evidence + scores + regime context
   - Output: Alerts (DB + JSONL) with stable IDs; deduped
7) **ReportAgent**
   - Input: Alerts since last report window + regime summary
   - Output: twice-daily email-ready report drafts; track report_state

### 5.2 Data flow (conceptual)
`Collectors → EventLog → Parser → Evidence → Resolver → Linked Evidence → Scorer (+ states) → RegimeAdapter (+ snapshot) → AlertAgent → Alerts → ReportAgent → Email draft`

---

## 6) Data Model (DB + Files)

### 6.1 File outputs
- `/out/alerts.jsonl` (deterministic ordering)
- `/out/report_preview_AM.md` and `/out/report_preview_PM.md` (dry-run previews)

### 6.2 DB (Postgres, Phase 2+)
Minimum tables (logical; exact columns defined during Phase 2):
- event_log (RawEvents)
- evidence
- companies, assets, rights
- company_assets, asset_rights
- company_state, asset_state
- catalyst_calendar
- alerts
- report_state/report_runs
- environment_snapshots (optional; at least latest snapshot stored)

---

## 7) Scoring & Health Model

### 7.1 MicroScore (company/asset)
A composite score representing optionality + catalyst strength:
- Catalyst strength (permitting, leases, drills, assays, completions)
- Evidence quality (source credibility, extraction confidence)
- Stage & proximity to value inflection (rights → drill → result)
- Operator quality / track record proxy (can be stubbed MVP)
- Liquidity / float / market structure proxy (optional MVP)
- **Health / survivability** (see below)
Output must include:
- sub-scores
- total score
- `features_used` list
- `explanations` (human-readable, concise)

### 7.2 Health / survivability (required)
Compute and store:
- Cash runway days (base/catalyst/stress)
- Cash burn rate proxy (monthly)
- Debt load & maturity risk proxy
- Dilution/financing frequency proxy
- Flags (e.g., “needs financing < 90d”, “high dilution risk”, “going concern language” if sourced)

MVP acceptable approach:
- Use simplified heuristics if full financial ingest isn’t built yet, but the fields and explanation scaffolding must exist.

---

## 8) Regime Engine Hook (Macro)

### 8.1 Goal
Allow Track A scoring/alerting to incorporate a holistic picture of the investment environment.

### 8.2 Interface
- `EnvironmentSnapshot`: timestamped regime state + factor surface
- `ExposureProfile`: mapping from vertical_tags / commodity_tags to factors
- `AdjustedScore`: MicroScore adjusted by regime

MVP:
- load a stub `/samples/regime_snapshot_stub.json`
- implement a basic exposure mapping (config-driven preferred)
- include regime summary text in alerts and reports

---

## 9) Alerts & Intraday Triggers

### 9.1 Alert structure (required fields)
Each alert must include:
- `alert_id` (stable hash)
- `tier` ("TIER1" | "TIER2")
- `event_type` (string)
- `event_time` (nullable)
- `ingest_time` (non-null)
- `company_id` (nullable)
- `asset_id` (nullable)
- `canonical_doc_id` (string)
- `evidence_pointer` (EvidencePointer)
- `score_summary` (micro + adjusted, plus key sub-scores)
- `summary` (1–3 sentences)
- `details` (structured json with extracted facts and reasoning)
- `regime_context` (short text or structured factors used)

### 9.2 Tiering (MVP)
- Tier-1 if MicroScore >= threshold OR event_type in a “high signal” list (e.g., drilling permit issued, spud, completion, material assay, key lease award, major financing with strategic investor)
- Tier-2 otherwise (watchlist / weaker signals)

### 9.3 Intraday trigger philosophy
- Triggers are evidence-driven and deterministic.
- Alerts are produced quickly; human reviews before trading.
- The system may later integrate “trade suggestion” stubs, but execution is out of scope for MVP.

---

## 10) Reporting (Twice Daily Email-Ready)

### 10.1 Output requirement
System must generate two report drafts per day:
- AM report (e.g., 07:00 local configurable)
- PM report (e.g., 19:00 local configurable)

MVP: generate markdown previews in `/out/` and a console print; email sending can be stubbed until integration.

### 10.2 Report content (minimum)
- Top Tier-1 alerts since last report window
- Top Tier-2 watchlist alerts since last report window
- Catalyst calendar: upcoming 7 days and 30 days
- Regime summary: “what environment are we in” (from EnvironmentSnapshot)
- Company health flags: “runway under X days”, “near-term debt maturity”, “dilution risk”
- Each item links back to evidence pointers (canonical_doc_id and source pointer details)

---

## 11) Implementation Shape

### 11.1 Language & structure
- Python
- Package root: `ogree_alpha/` (no `app/`)
- CLI entry: `python -m ogree_alpha`
- Config: `config/universe.yaml` (initial watchlist universe)

### 11.2 Compatibility
- Prefer real deps: pydantic v2, pydantic-settings, typer, PyYAML.
- If compat shims are needed:
  - isolate under `ogree_alpha/_compat/`
  - all imports routed through `ogree_alpha/compat.py`
  - enforce via tests that no module outside compat imports `_compat`

### 11.3 Output directories
- `/samples/` for offline demo artifacts
- `/out/` for generated outputs

---

## 12) Phases & Acceptance Criteria

### Phase 1 — Contracts + Protocols + CLI Skeleton
Deliver:
- Pydantic contracts: EvidencePointer, EnvironmentSnapshot, Alert, ReportDraft
- Protocol interfaces for agents
- Stable hashing utilities (canonical_doc_id, alert_id)
- UniverseManager loads config/universe.yaml
- Typer CLI skeleton with stubs
Acceptance:
- `python -m ogree_alpha --help` works
- `pytest` passes

### Phase 2 — Postgres + Alembic + Repository Layer
Deliver:
- docker-compose Postgres (or documented alternative)
- SQLAlchemy models + Alembic migrations
- Repository functions with idempotent and concurrency-safe inserts using ON CONFLICT
Acceptance:
- migrations apply successfully
- idempotency tests pass (raw events + alerts)

### Phase 3 — Offline End-to-End Demo
Deliver:
- `/samples/*` inputs (RRCTx + Alaska + mining PR stubs + regime snapshot stub)
- pipeline runs end-to-end producing >= 10 alerts
Acceptance:
- deterministic reruns produce byte-identical `/out/alerts.jsonl`
- no duplicate DB alerts on rerun

### Phase 4 — Scoring Depth + Backtest Harness
Deliver:
- deeper scoring heuristics
- backtest harness reading historical samples and evaluating signal performance

### Phase 5 — Regime Integration + Exposure Mapping
Deliver:
- improved environment snapshot ingestion
- factor exposure mapping and adjusted scoring
- macro team interface stubs

### Phase 6 — Reporting + Email Integration
Deliver:
- schedule-aware report generation
- email send integration (2x daily) with report_state tracking
Acceptance:
- two daily reports can be generated and (optionally) emailed on schedule


