#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

SMOKE_PREFIX="[smoke]"
DEFAULT_DATABASE_URL="postgresql://ogree:ogree@localhost:5432/ogree"
DATABASE_URL="${DATABASE_URL:-${DEFAULT_DATABASE_URL}}"
export DATABASE_URL

log() {
  printf "%s %s\n" "${SMOKE_PREFIX}" "$*"
}

fail() {
  printf "%s ERROR: %s\n" "${SMOKE_PREFIX}" "$*" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "${path}" ]] || fail "Required file missing: ${path}"
}

pick_python() {
  if [[ -x ".venv/bin/python" ]]; then
    echo ".venv/bin/python"
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    command -v python
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  return 1
}

pick_compose() {
  if command -v docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
    return 0
  fi
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    echo "docker compose"
    return 0
  fi
  return 1
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_s="$3"
  local python_bin="$4"

  "${python_bin}" - "${host}" "${port}" "${timeout_s}" <<'PY' || return 1
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
timeout_s = int(sys.argv[3])
deadline = time.time() + timeout_s

while time.time() < deadline:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1.0)
    try:
        sock.connect((host, port))
    except OSError:
        time.sleep(1.0)
    else:
        sock.close()
        sys.exit(0)
    finally:
        try:
            sock.close()
        except OSError:
            pass

sys.exit(1)
PY
}

require_file "docker-compose.yml"
require_file "alembic.ini"

PYTHON_BIN="$(pick_python)" || fail "Python not found. Install Python 3.12+."
log "Using Python: ${PYTHON_BIN}"

if ! "${PYTHON_BIN}" -c "import alembic, pytest, sqlalchemy, typer, yaml, psycopg2" >/dev/null 2>&1; then
  fail "Missing Python deps. Install with: python3 -m pip install --user pytest sqlalchemy alembic pydantic pydantic-settings typer pyyaml psycopg2-binary"
fi

COMPOSE_CMD="$(pick_compose)" || fail "Neither 'docker-compose' nor 'docker compose' is available."
log "Using compose command: ${COMPOSE_CMD}"

if [[ "${COMPOSE_CMD}" == "docker-compose" ]]; then
  if ! docker-compose version >/dev/null 2>&1; then
    fail "docker-compose exists but is not functional in this environment."
  fi
else
  if ! docker version >/dev/null 2>&1; then
    fail "docker CLI found but Docker daemon is unavailable."
  fi
fi

log "Bringing up Postgres container..."
if [[ "${COMPOSE_CMD}" == "docker-compose" ]]; then
  docker-compose up -d
else
  docker compose up -d
fi

log "Waiting for Postgres on localhost:5432..."
if ! wait_for_port "127.0.0.1" "5432" "60" "${PYTHON_BIN}"; then
  fail "Postgres did not become ready on localhost:5432 within 60 seconds."
fi

log "Running Alembic migrations..."
"${PYTHON_BIN}" -m alembic upgrade head

log "Running DB connectivity check..."
"${PYTHON_BIN}" -m ogree_alpha db-check

log "Running full pipeline (run-all)..."
"${PYTHON_BIN}" -m ogree_alpha run-all --hours 72 --report-hours 24 --top-n 25

log "Running pytest suite..."
"${PYTHON_BIN}" -m pytest tests/ -v

log "Dumping row counts..."
"${PYTHON_BIN}" - <<'PY'
import os
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])
with engine.begin() as conn:
    event_count = conn.execute(text("SELECT COUNT(*) FROM event_log")).scalar_one()
    alert_count = conn.execute(text("SELECT COUNT(*) FROM alerts")).scalar_one()

print(f"[smoke] event_log rows={event_count}")
print(f"[smoke] alerts rows={alert_count}")
PY

log "Smoke test completed successfully."
