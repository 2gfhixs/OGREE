LIVE_URL: str = "https://example.invalid/alaska/permits"  # placeholder
from typing import Any, Dict
def fetch_live(url: str = LIVE_URL, timeout_s: int = 10) -> Dict[str, Any]:
    try:
        import requests  # type: ignore
    except Exception as e:
        raise RuntimeError("requests not installed") from e

    r = requests.get(url, timeout=timeout_s)
    if r.status_code != 200:
        raise RuntimeError(f"fetch_live status={r.status_code}")
    return r.json()
