# OGREE Milestones and Next Steps

This document captures the practical roadmap from current state to a mature, agent-driven research platform.

---

## Current State (as of this branch)

Implemented and running:
- Core ingestion agents (AK, TX, REE/U, SEC fixture/live)
- Policy signal agents (Federal Register final rules, NPRM/Congressional fixtures)
- Convergence scoring and alert generation
- Opportunity ranking and report generation
- Exportable email report artifact (`.eml`) with optional SMTP send
- Smoke workflow (local + CI) and pipeline health command

---

## Milestone Plan

## M1 — Agent Runtime Hardening (Now)
**Goal:** Make individual agents reliable, observable, and replay-safe.

### Deliverables
- Agent orchestration runbook (schedule, retry, DLQ policy)
- Per-agent health metrics (lag, parse success, skip rates)
- Replay tooling by source + time range
- Baseline SLOs for ingestion and alert generation latency

### Exit Criteria
- Any single agent can fail and recover without taking down the full pipeline.
- Health dashboard indicates data freshness and parsing quality per source.

---

## M2 — Live Policy Intelligence Agents
**Goal:** Upgrade policy fixtures to live connectors.

### Deliverables
- Federal Register live polling for final rules
- Regulations.gov NPRM + public comment window monitoring
- Congressional feed integration:
  - trading disclosures
  - committee bill movement events
- Confidence and impact tagging (`favorable`, `adverse`, `mixed`, `neutral`)

### Exit Criteria
- Policy events are generated automatically and flow into convergence category F.
- Alert summaries include live policy context with timestamps and source links.

---

## M3 — Alpha/Backtesting Agent
**Goal:** Quantify signal quality and improve expected value.

### Deliverables
- Event-time backtester (no look-ahead)
- Holding-window evaluations (5d/20d/60d)
- Signal attribution by category (A-F)
- Parameter tuning loop for convergence and tier thresholds

### Exit Criteria
- Weekly backtest report shows performance by signal category and regime.
- Changes to scoring are gated by backtest deltas, not intuition alone.

---

## M4 — Team Workflow Layer
**Goal:** Make outputs actionable for daily use.

### Deliverables
- Alert triage workflow (ack/dismiss/watch)
- Opportunity board with evidence drill-down by contributing agent
- Team feedback capture loop for false positives/false negatives

### Exit Criteria
- Team can operate from a single dashboard/report flow each day.
- Feedback systematically updates prioritization and tuning backlog.

---

## Immediate Next Steps (Recommended Sequence)

1. **Stabilize live SEC runtime** with per-company progress logs and timeout diagnostics in command output.
2. **Implement live Federal Register connector** with pagination + dedupe.
3. **Implement live NPRM/comment connector** (Regulations.gov) with comment deadline alerts.
4. **Add congressional feed connector** for trade disclosures + committee events.
5. **Publish weekly alpha quality report** from backtesting agent.
6. **Add lightweight demo dashboard** (or notebook output pack) for team operations.

---

## Execution Model (Avoiding Single-Program Overload)

Treat OGREE as a coordinated multi-agent system:

- **Ingestion agents:** source-specific extraction/normalization
- **Scoring agents:** convergence + alert production
- **Delivery agents:** report/email/export
- **Observability agent:** health, lag, parse stats
- **Backtesting agent:** performance and calibration

This keeps each module focused, testable, and replaceable without destabilizing the whole system.

---

## Demo-Ready Commands

```bash
DATABASE_URL="postgresql://ogree:ogree@localhost:5432/ogree" python -m ogree_alpha run-all
DATABASE_URL="postgresql://ogree:ogree@localhost:5432/ogree" python -m ogree_alpha health --hours 72 --alert-hours 24
DATABASE_URL="postgresql://ogree:ogree@localhost:5432/ogree" python -m ogree_alpha email-report --to "warhound762@gmail.com" --output output/demo_report.eml
```
