from __future__ import annotations

import json
import os
from typing import Optional

import typer
from sqlalchemy import text

from ogree_alpha.db.session import get_session

app = typer.Typer(add_completion=False, help="OGREE Exploration Alpha — CLI")


@app.command("db-check")
def db_check() -> None:
    """Check DB connectivity and print basic info."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise typer.BadParameter("DATABASE_URL is not set")

    with get_session() as session:
        session.execute(text("SELECT 1"))
        version = session.execute(text("SELECT version()")).scalar_one()
        typer.echo("DB OK")
        typer.echo(str(version))


@app.command("ingest-demo")
def ingest_demo(
    path: str = typer.Option("sample_data/raw_events.jsonl", help="Path to demo JSONL"),
) -> None:
    """Run the demo pipeline: ingest sample events and generate alerts."""
    from ogree_alpha.demo_pipeline import ingest_and_alert

    out = ingest_and_alert(path)
    typer.echo(f"Processed {len(out)} events")
    inserted_raw = sum(1 for o in out if o["raw_event"]["inserted"])
    inserted_alerts = sum(1 for o in out if o["alert"]["inserted"])
    typer.echo(f"  raw events inserted: {inserted_raw}")
    typer.echo(f"  alerts inserted:     {inserted_alerts}")


@app.command("ingest-ak")
def ingest_ak() -> None:
    """Ingest Alaska permits + wells into event_log."""
    from ogree_alpha.adapters.alaska_permits import ingest_zip_fixture_to_db as ingest_permits
    from ogree_alpha.adapters.alaska_wells import ingest_zip_fixture_to_db as ingest_wells

    n_permits = ingest_permits()
    typer.echo(f"AK permits: {n_permits} new events inserted")

    n_wells = ingest_wells()
    typer.echo(f"AK wells:   {n_wells} new events inserted")


@app.command("ingest-tx")
def ingest_tx(
    path: str = typer.Option(
        "sample_data/texas/rrc_raw_events.jsonl",
        help="Path to TX RRC fixture JSONL",
    ),
) -> None:
    """Ingest Texas RRC fixture events into event_log."""
    from ogree_alpha.adapters.texas_rrc import ingest_fixture_to_db

    inserted, processed = ingest_fixture_to_db(path)
    typer.echo(f"TX RRC: processed {processed}, inserted {inserted} new events")


@app.command("ingest-ree")
def ingest_ree(
    path: str = typer.Option(
        "sample_data/ree_uranium/events.jsonl",
        help="Path to REE/Uranium fixture JSONL",
    ),
) -> None:
    """Ingest REE + Uranium fixture events into event_log."""
    from ogree_alpha.adapters.ree_uranium import ingest_fixture_to_db

    inserted, processed = ingest_fixture_to_db(path)
    typer.echo(f"REE/U: processed {processed}, inserted {inserted} new events")


@app.command("ingest-sec")
def ingest_sec(
    path: str = typer.Option(
        "sample_data/sec_edgar/form4_events.jsonl",
        help="Path to SEC EDGAR fixture JSONL",
    ),
) -> None:
    """Ingest SEC EDGAR insider/institutional fixture events into event_log."""
    from ogree_alpha.adapters.sec_edgar import ingest_fixture_to_db

    inserted, processed = ingest_fixture_to_db(path)
    typer.echo(f"SEC EDGAR: processed {processed}, inserted {inserted} new events")


@app.command("ingest-sec-live")
def ingest_sec_live(
    max_filings_per_company: int = typer.Option(20, help="Max filings per company"),
    user_agent: str = typer.Option(
        "OGREE/0.1 (research@ogree.local)",
        help="SEC-required User-Agent header",
    ),
    timeout_s: int = typer.Option(20, help="HTTP timeout in seconds"),
    request_delay_s: float = typer.Option(0.2, help="Delay between SEC HTTP requests (seconds)"),
    max_retries: int = typer.Option(3, help="Max retries for retryable SEC HTTP errors"),
    backoff_base_s: float = typer.Option(1.0, help="Base backoff seconds for SEC HTTP retries"),
    universe_path: str = typer.Option("config/universe.yaml", help="Path to universe YAML"),
) -> None:
    """Ingest recent SEC EDGAR filings from SEC submissions endpoints."""
    from ogree_alpha.adapters.sec_edgar import ingest_live_to_db_with_stats

    inserted, processed, stats = ingest_live_to_db_with_stats(
        universe_path=universe_path,
        user_agent=user_agent,
        max_filings_per_company=max_filings_per_company,
        timeout_s=timeout_s,
        request_delay_s=request_delay_s,
        max_retries=max_retries,
        backoff_base_s=backoff_base_s,
    )
    typer.echo(f"SEC EDGAR live: processed {processed}, inserted {inserted} new events")
    typer.echo(
        "  Form 4 filings: "
        f"seen={stats.get('form4_filings_seen', 0)} "
        f"parsed={stats.get('form4_filings_parsed', 0)} "
        f"skipped={stats.get('form4_filings_skipped', 0)} "
        f"tx_emitted={stats.get('form4_transactions_emitted', 0)}"
    )
    typer.echo(f"  Institutional events emitted: {stats.get('institutional_events_emitted', 0)}")


@app.command("generate-alerts")
def generate_alerts(
    hours: int = typer.Option(72, help="Lookback window in hours"),
    top_n: int = typer.Option(25, help="Max alerts to generate"),
) -> None:
    """Score recent events and generate alerts."""
    from ogree_alpha.alert_generator import generate_and_insert_alerts

    n = generate_and_insert_alerts(hours=hours, top_n=top_n)
    typer.echo(f"Alerts: {n} new alerts inserted")


@app.command("report")
def report(
    hours: int = typer.Option(24, help="Lookback window in hours"),
    top_n: int = typer.Option(10, help="Max items per section"),
    output: Optional[str] = typer.Option(None, help="Write report JSON to file"),
) -> None:
    """Generate a twice-daily report (text + HTML)."""
    from ogree_alpha.report_twice_daily import render_report

    result = render_report(hours=hours, top_n=top_n)
    if output:
        with open(output, "w", encoding="utf-8") as f:
            json.dump(result, f, default=str, indent=2)
        typer.echo(f"Report written to {output}")
    else:
        typer.echo(f"Subject: {result['subject']}")
        typer.echo("")
        typer.echo(result["text"])


@app.command("opportunities")
def opportunities(
    hours: int = typer.Option(24, help="Lookback window in hours"),
    top_n: int = typer.Option(15, help="Max opportunities"),
) -> None:
    """Rank and display top opportunities."""
    from ogree_alpha.opportunity_ranker import rank_opportunities, render_text

    opps = rank_opportunities(hours=hours, top_n=top_n)
    typer.echo(render_text(opps))


@app.command("health")
def health(
    hours: int = typer.Option(72, help="Event window in hours"),
    alert_hours: int = typer.Option(24, help="Alert window in hours"),
    output: Optional[str] = typer.Option(None, help="Write health snapshot JSON to file"),
) -> None:
    """Compute and display pipeline health metrics."""
    from ogree_alpha.observability import compute_health_snapshot, render_text

    snapshot = compute_health_snapshot(hours=hours, alert_hours=alert_hours)
    if output:
        with open(output, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
        typer.echo(f"Health snapshot written to {output}")
    else:
        typer.echo(render_text(snapshot))


@app.command("run-all")
def run_all(
    hours: int = typer.Option(72, help="Event lookback window in hours"),
    report_hours: int = typer.Option(24, help="Report lookback window in hours"),
    top_n: int = typer.Option(25, help="Max alerts"),
    report_file: Optional[str] = typer.Option(None, help="Write report to file"),
    sec_live: bool = typer.Option(False, help="Also ingest live SEC EDGAR submissions"),
    sec_live_max_filings_per_company: int = typer.Option(
        20,
        help="Max live SEC filings per company (used with --sec-live)",
    ),
    sec_live_user_agent: str = typer.Option(
        "OGREE/0.1 (research@ogree.local)",
        help="SEC-required User-Agent for live SEC mode (used with --sec-live)",
    ),
    sec_live_timeout_s: int = typer.Option(
        20,
        help="HTTP timeout seconds for live SEC mode (used with --sec-live)",
    ),
    sec_live_request_delay_s: float = typer.Option(
        0.2,
        help="Delay between SEC live HTTP requests seconds (used with --sec-live)",
    ),
    sec_live_max_retries: int = typer.Option(
        3,
        help="Max retry attempts for SEC live HTTP errors (used with --sec-live)",
    ),
    sec_live_backoff_base_s: float = typer.Option(
        1.0,
        help="Base backoff seconds for SEC live retries (used with --sec-live)",
    ),
    sec_live_universe_path: str = typer.Option(
        "config/universe.yaml",
        help="Universe path for live SEC mode (used with --sec-live)",
    ),
) -> None:
    """Run full pipeline: ingest fixture sources (+optional live SEC) -> alerts -> report."""
    sec_live_enabled = sec_live if isinstance(sec_live, bool) else bool(getattr(sec_live, "default", False))

    typer.echo("=== Ingest: Demo ===")
    ingest_demo(path="sample_data/raw_events.jsonl")

    typer.echo("\n=== Ingest: Alaska ===")
    ingest_ak()

    typer.echo("\n=== Ingest: Texas ===")
    ingest_tx(path="sample_data/texas/rrc_raw_events.jsonl")

    typer.echo("\n=== Ingest: REE/Uranium ===")
    ingest_ree(path="sample_data/ree_uranium/events.jsonl")

    typer.echo("\n=== Ingest: SEC EDGAR ===")
    ingest_sec(path="sample_data/sec_edgar/form4_events.jsonl")

    if sec_live_enabled:
        typer.echo("\n=== Ingest: SEC EDGAR (live) ===")
        ingest_sec_live(
            max_filings_per_company=sec_live_max_filings_per_company,
            user_agent=sec_live_user_agent,
            timeout_s=sec_live_timeout_s,
            request_delay_s=sec_live_request_delay_s,
            max_retries=sec_live_max_retries,
            backoff_base_s=sec_live_backoff_base_s,
            universe_path=sec_live_universe_path,
        )

    typer.echo("\n=== Generate Alerts ===")
    generate_alerts(hours=hours, top_n=top_n)

    typer.echo("\n=== Report ===")
    report(hours=report_hours, top_n=10, output=report_file)

    typer.echo("\n=== Top Opportunities ===")
    opportunities(hours=report_hours, top_n=15)

    typer.echo("\nDone.")
