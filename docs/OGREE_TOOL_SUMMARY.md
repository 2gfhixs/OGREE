# OGREE Tool Summary (Current State)

## What OGREE Is

OGREE is an event-driven research engine focused on finding high-optionality setups in small-cap natural resources by tracking the chain:

1. rights/permit/claims activity  
2. drill/results progress  
3. financing/deal and policy context  
4. insider/institutional accumulation

It produces alerts, ranked opportunities, and report output suitable for human trading review.

---

## Current Agent-Oriented Modules

The codebase is structured so each source can run as its own focused agent process:

- **AK Agent** (`alaska_permits`, `alaska_wells`)
- **TX Agent** (`texas_rrc`)
- **REE/U Agent** (`ree_uranium`)
- **SEC Agent** (`sec_edgar`)
  - fixture ingest
  - live SEC submissions ingest
  - Form 4 transaction parsing (P/S/M)
- **Federal Register Rules Agent** (`federal_register_rules`)
- **NPRM + Congressional Agent** (`nprm_congressional`)
- **Convergence/Scoring Agent** (`chain_view`, `convergence`)
- **Alert/Ranking/Report Agent**
  - `alert_generator`
  - `opportunity_ranker`
  - `report_twice_daily`
- **Observability/Health Agent** (`observability`)

---

## Scoring and Convergence

### Base progression scoring
- Permit/claims
- Spud/stage progression
- Well/drill/assay
- Production (where available)

### Additive signals
- Resource/study/deal/policy
- Insider buy and insider cluster boost

### Convergence pass
Distinct category count in rolling window (default 30 days):
- A: permits/claims
- B: drill/results/completions
- C: resource/study
- D: financing/deals
- E: insider/institutional
- F: policy/macro/regulatory/congressional

Output includes:
- `convergence_score`
- `convergence_categories`
- summary annotation when convergence >= 3

---

## Output and Delivery Features

- CLI end-to-end pipeline (`run-all`)
- Optional live SEC pass during run-all (`--sec-live`)
- Health snapshot command (`health`)
- Exportable email report generation (`email-report`)
  - write `.eml` file
  - optional SMTP send

---

## Reliability and Safety Controls

- Idempotent DB inserts (raw events + alerts)
- Deterministic IDs/hashing
- Replay-safe fixture paths
- SEC live safety layer:
  - request pacing
  - retry with exponential backoff
  - configurable timeout/retry/backoff knobs
  - fail-fast bounds validation on unsafe parameter values
  - Form 4 parse diagnostics (seen/parsed/skipped/emitted)

---

## CI and Test Status

- GitHub Actions smoke workflow with Postgres service
- Full test suite currently green:
  - **80 passed, 10 skipped** (latest run in this branch)

---

## What This Means for the Team Today

You can demo now with:

1. full pipeline ingestion and scoring
2. policy + SEC insider signal integration
3. convergence-aware opportunity ranking
4. exportable email-style report artifact (`.eml`)

This is already useful as a collaborative research cockpit while live-data agents are expanded and hardened.
