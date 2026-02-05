from __future__ import annotations

import os

import typer
from sqlalchemy import text

from ogree_alpha.db.session import get_session

app = typer.Typer(add_completion=False)


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


if __name__ == "__main__":
    app()
