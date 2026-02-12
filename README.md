# OGREE

Exploration Alpha (Track A): event-driven research engine for oil/gas + REE/uranium.

See `SPEC.md` for the full system specification.


## Smoke test

Run a full local E2E smoke (DB + migrations + pipeline + tests):

```bash
make smoke
```

What `make smoke` does:
- Preflights Python + required packages
- Preflights `docker-compose` or `docker compose`
- Starts Postgres from `docker-compose.yml`
- Waits for `localhost:5432` readiness
- Runs `alembic upgrade head`
- Runs `python -m ogree_alpha db-check`
- Runs `python -m ogree_alpha run-all`
- Runs `pytest tests/ -v`

You can override the default DB URL:

```bash
DATABASE_URL="postgresql://ogree:ogree@localhost:5432/ogree" make smoke
```


## Notes / Known caveats

- `insert_raw_event(...)` in `ogree_alpha/db/repo.py` assumes `raw_event["source_event_id"]` is **non-null** when handling the conflict/idempotency path.  
  If `source_event_id` is `None`, there is **no uniqueness guarantee** (partial unique index only applies when `source_event_id IS NOT NULL`), and the “fetch existing_id” lookup is not valid.  
  **Rule:** Only call `insert_raw_event` for events that have a stable `source_event_id`, or extend the repo logic to support a secondary idempotency key (e.g., `content_hash`) for null IDs.
