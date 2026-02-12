# OGREE Demo Report (Sample)

This is a sample of the kind of report OGREE can generate today after running:

```bash
DATABASE_URL="postgresql://ogree:ogree@localhost:5432/ogree" python -m ogree_alpha run-all
DATABASE_URL="postgresql://ogree:ogree@localhost:5432/ogree" python -m ogree_alpha report --hours 24 --top-n 10
```

---

## Subject

`OGREE Alpha - Top Alerts - 2026-02-12`

## Top Opportunities

Rank | Score | Tier | Ticker(s) | Summary
---- | ----- | ---- | --------- | -------
1 | 1.00 | high | PR | [HIGH] chain progression 900001 (Permian Resources Corporation, Texas) score=1.0 convergence=3 [A,B,E]
2 | 0.90 | high | UCU.V,UURAF | [HIGH] chain progression AK-EXP-2025-4421 (Ucore Rare Metals Inc., Alaska) score=0.9 convergence=4 [B,C,D,E]
3 | 0.90 | high | URG,URE.TO | [HIGH] chain progression WY-MIN-2025-0089 (Ur-Energy Inc., Wyoming) score=0.9 convergence=4 [B,C,D,E]
4 | 0.80 | high | MGY | [HIGH] chain progression 900010 (Magnolia Oil & Gas Corporation, Texas) score=0.8 convergence=3 [A,B,E]
5 | 0.60 | medium | REI | [MEDIUM] chain progression 900030 (Ring Energy, Inc., Texas) score=0.6

## Top Alerts

### HIGH
- chain progression TX:42-301-00001 (Permian Resources Corporation, Texas) score=1.0 convergence=3 [A,B,E]
- chain progression Ucore Bokan-Dotson Ridge (Ucore Rare Metals Inc., Alaska) score=0.9 convergence=4 [B,C,D,E]
- chain progression Ur-Energy Lost Creek (Ur-Energy Inc., Wyoming) score=0.9 convergence=4 [B,C,D,E]
- chain progression TX:42-383-00042 (Magnolia Oil & Gas Corporation, Texas) score=0.8 convergence=3 [A,B,E]

### MEDIUM
- chain progression TX:42-461-00005 (Ring Energy, Inc., Texas) score=0.6
- policy_final_rule DOE-HQ-2025-0020 (Ucore Rare Metals Inc., US) impact=favorable

### LOW
- chain progression TX:42-227-00099 (Comstock Resources, Inc., Texas) score=0.3
- policy_nprm_open BLM-2026-0007 (Ucore Rare Metals Inc., US) impact=favorable

## Policy Signal Highlights

- **Federal Register final rules** included in scoring:
  - NRC uranium licensing rule (Ur-Energy) - favorable
  - EPA methane standards (Permian Resources) - adverse
  - DOE critical mineral tax credit implementation (Ucore) - favorable
- **NPRM / congressional stream** included in scoring:
  - NPRM opening and comment deadlines (critical minerals, pipeline emissions)
  - Congressional trade disclosures
  - Committee advancement of relevant legislation (domestic uranium)

## SEC Insider Signal Highlights

- Form 4 live ingestion supports parsed transaction-level events:
  - `P` -> insider_buy
  - `S` -> insider_sell
  - `M` -> insider_option_exercise
- SEC live run prints parse diagnostics:
  - filings seen / parsed / skipped
  - transactions emitted
  - institutional events emitted (13G/13F)

---

## Demo Export

Generate an email-style export file for team demo:

```bash
DATABASE_URL="postgresql://ogree:ogree@localhost:5432/ogree" \
python -m ogree_alpha email-report \
  --to "warhound762@gmail.com" \
  --output output/demo_report.eml
```

This creates an RFC822 `.eml` report that can be opened/imported in email clients.
