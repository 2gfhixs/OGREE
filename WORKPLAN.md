
## Phase 7 — Top Opportunities Output (Complete)
**Outcome:** Twice-daily report now includes a "Top Opportunities" section (ranked shortlist) plus tiered alerts.
**Key code:**
- `ogree_alpha/opportunity_ranker.py` — ranks recent alert-derived opportunities and renders a table
- `ogree_alpha/report_twice_daily.py` — deterministic report formatting, prepends opportunities + dedupes alerts
**Acceptance:** `pytest -q` green; report renders `subject/text/html` with opportunities first.

## Phase 8 — Investable Entity Resolution + Thematic Buckets (Planned)
**Goal:** Convert placeholders into investable outputs (real tickers, entity identity, and evidence-driven catalyst notes).

### 8.1 Entity Resolution Layer
- Add `ogree_alpha/entity_resolution.py`
- Inputs: company names, operator names, permit/well metadata, universe aliases
- Outputs: `company_id`, `primary_ticker`, `ticker_set`, confidence score, matched alias

### 8.2 Universe Expansion for Real Entities
- Extend `config/universe.yaml` to include:
  - company aliases (name changes, OTC/TSX-V dual listings)
  - operator aliases (for permits/wells)
  - asset tags (AK, TX, REE, U)
- Add validations/tests for universe completeness.

### 8.3 Thematic Bucketing in Report
- Group "Top Opportunities" by theme:
  - AK Gas / Permits & Wells
  - TX Gas / RRC (parser will be messy)
  - REE / Uranium (press releases first; NI 43-101 later)
- Each bucket includes: tickers, catalyst stage, evidence pointer summary, score.

### 8.4 Company Health Stub (Optional in Phase 8)
- Add placeholder fields (cash, debt, burn, dilution flags) to opportunity output.
- Implement as stub inputs now; wire real data sources later.

