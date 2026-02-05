# OGREE

Exploration Alpha (Track A): event-driven research engine for oil/gas + REE/uranium.

See `SPEC.md` for the full system specification.


## Notes / Known caveats

- `insert_raw_event(...)` in `ogree_alpha/db/repo.py` assumes `raw_event["source_event_id"]` is **non-null** when handling the conflict/idempotency path.  
  If `source_event_id` is `None`, there is **no uniqueness guarantee** (partial unique index only applies when `source_event_id IS NOT NULL`), and the “fetch existing_id” lookup is not valid.  
  **Rule:** Only call `insert_raw_event` for events that have a stable `source_event_id`, or extend the repo logic to support a secondary idempotency key (e.g., `content_hash`) for null IDs.
