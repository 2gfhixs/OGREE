"""SEC EDGAR adapter + insider signal scoring tests."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from ogree_alpha.adapters.sec_edgar import (
    SEC_SUBMISSIONS_URL_TEMPLATE,
    SEC_TICKER_MAP_URL,
    _load_ticker_to_cik_map,
    _classify_form_event_type,
    _canonicalize_payload,
    _derive_lineage_id,
    _normalize_relationship,
    _normalize_type,
    _normalize_transaction_type,
    _parse_dt,
    parse_form4_transactions,
    iter_fixture_events,
)
from ogree_alpha.chain_view import compute_chain_scores


def test_normalize_type_aliases():
    assert _normalize_type("insider_buy") == "insider_buy"
    assert _normalize_type("purchase") == "insider_buy"
    assert _normalize_type("sell") == "insider_sell"
    assert _normalize_type("13G") == "institutional_13g"
    assert _normalize_type("form_13f") == "institutional_13f"


def test_normalize_relationship():
    assert _normalize_relationship("Chief Financial Officer") == "officer"
    assert _normalize_relationship("Director") == "director"
    assert _normalize_relationship("10 percent beneficial owner") == "10% owner"
    assert _normalize_relationship("Institutional Fund") == "institution"


def test_normalize_transaction_type_fallbacks():
    assert _normalize_transaction_type(None, normalized_event_type="insider_buy") == "purchase"
    assert _normalize_transaction_type(None, normalized_event_type="insider_sell") == "sale"
    assert _normalize_transaction_type(None, normalized_event_type="insider_option_exercise") == "exercise"
    assert _normalize_transaction_type("buy", normalized_event_type="institutional_13g") == "purchase"


def test_derive_lineage_prefers_company_id():
    payload = {"company_id": "PERMIAN_RESOURCES", "company": "Permian Resources Corporation"}
    assert _derive_lineage_id(payload) == "SEC:PERMIAN_RESOURCES"


def test_classify_form_event_type():
    assert _classify_form_event_type("4") == "form4"
    assert _classify_form_event_type("4/A") == "form4"
    assert _classify_form_event_type("SC 13G") == "institutional_13g"
    assert _classify_form_event_type("13F-HR") == "institutional_13f"
    assert _classify_form_event_type("8-K") is None


def test_parse_form4_transactions_buy_sell_exercise():
    xml = """
    <ownershipDocument>
      <reportingOwner>
        <reportingOwnerId>
          <rptOwnerName>Jane Q Doe</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship>
          <isDirector>1</isDirector>
          <isOfficer>1</isOfficer>
          <isTenPercentOwner>0</isTenPercentOwner>
          <isOther>0</isOther>
          <officerTitle>CEO</officerTitle>
        </reportingOwnerRelationship>
      </reportingOwner>
      <nonDerivativeTable>
        <nonDerivativeTransaction>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionDate><value>2026-02-01</value></transactionDate>
          <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>10000</value></transactionShares>
            <transactionPricePerShare><value>2.5</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
          <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
        </nonDerivativeTransaction>
        <nonDerivativeTransaction>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionDate><value>2026-02-02</value></transactionDate>
          <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>5000</value></transactionShares>
            <transactionPricePerShare><value>2.9</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
        </nonDerivativeTransaction>
      </nonDerivativeTable>
      <derivativeTable>
        <derivativeTransaction>
          <securityTitle><value>Option (right to buy)</value></securityTitle>
          <transactionDate><value>2026-02-03</value></transactionDate>
          <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>7000</value></transactionShares>
            <transactionPricePerShare><value>1.1</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
          <underlyingSecurity>
            <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
          </underlyingSecurity>
        </derivativeTransaction>
      </derivativeTable>
    </ownershipDocument>
    """
    rows = parse_form4_transactions(xml)
    assert len(rows) == 3
    types = {r.get("event_type") for r in rows}
    assert types == {"insider_buy", "insider_sell", "insider_option_exercise"}
    assert all(r.get("filer_name") == "Jane Q Doe" for r in rows)
    assert all(r.get("relationship") == "officer/director" for r in rows)


def test_canonicalize_payload_resolves_company_and_computes_total():
    payload = _canonicalize_payload(
        {
            "type": "insider_buy",
            "filer_name": "Dana Morgan",
            "relationship": "Chief Financial Officer",
            "company": "Permian Resources Corporation",
            "tickers": "",
            "shares": 60000,
            "price_per_share": 2.1,
        }
    )
    assert payload["company_id"] == "PERMIAN_RESOURCES"
    assert payload["lineage_id"] == "SEC:PERMIAN_RESOURCES"
    assert payload["transaction_type"] == "purchase"
    assert payload["total_value"] == 126000.0
    assert payload["tickers"] == ["PR"]


def test_iter_live_events_from_mocked_submissions(monkeypatch):
    from ogree_alpha.adapters import sec_edgar

    ticker_map = {
        "0": {"cik_str": 1024, "ticker": "PR", "title": "Permian Resources Corporation"},
    }
    submissions = {
        "filings": {
            "recent": {
                "form": ["4", "SC 13G", "8-K"],
                "accessionNumber": [
                    "0001024-26-000001",
                    "0001024-26-000002",
                    "0001024-26-000003",
                ],
                "filingDate": ["2026-02-01", "2026-02-05", "2026-02-10"],
                "primaryDocument": ["xslF345X03/doc1.xml", "doc2.txt", "doc3.htm"],
            }
        }
    }
    form4_xml = """
    <ownershipDocument>
      <reportingOwner>
        <reportingOwnerId><rptOwnerName>Dana Morgan</rptOwnerName></reportingOwnerId>
        <reportingOwnerRelationship>
          <isDirector>1</isDirector>
          <isOfficer>1</isOfficer>
          <isTenPercentOwner>0</isTenPercentOwner>
        </reportingOwnerRelationship>
      </reportingOwner>
      <nonDerivativeTable>
        <nonDerivativeTransaction>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionDate><value>2026-02-01</value></transactionDate>
          <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>10000</value></transactionShares>
            <transactionPricePerShare><value>13.1</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
        </nonDerivativeTransaction>
        <nonDerivativeTransaction>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionDate><value>2026-02-02</value></transactionDate>
          <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>2500</value></transactionShares>
            <transactionPricePerShare><value>13.6</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
        </nonDerivativeTransaction>
      </nonDerivativeTable>
      <derivativeTable>
        <derivativeTransaction>
          <securityTitle><value>Option</value></securityTitle>
          <transactionDate><value>2026-02-03</value></transactionDate>
          <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>7000</value></transactionShares>
            <transactionPricePerShare><value>8.0</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
          <underlyingSecurity>
            <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
          </underlyingSecurity>
        </derivativeTransaction>
      </derivativeTable>
    </ownershipDocument>
    """

    def _fake_http_get_json(url: str, *, user_agent: str, timeout_s: int = 20):
        if url == SEC_TICKER_MAP_URL:
            return ticker_map
        if url == SEC_SUBMISSIONS_URL_TEMPLATE.format(cik="0000001024"):
            return submissions
        return {}

    def _fake_http_get_text(url: str, *, user_agent: str, timeout_s: int = 20):
        if "doc1.xml" in url:
            return form4_xml
        return ""

    monkeypatch.setattr(sec_edgar, "_http_get_json", _fake_http_get_json)
    monkeypatch.setattr(sec_edgar, "_http_get_text", _fake_http_get_text)

    stats = {}
    out = list(
        sec_edgar.iter_live_events(
            user_agent="OGREE Test (test@example.com)",
            max_filings_per_company=10,
            timeout_s=1,
            stats=stats,
        )
    )
    assert len(out) == 4
    types = [o.get("payload_json", {}).get("type") for o in out]
    assert "insider_buy" in types
    assert "insider_sell" in types
    assert "insider_option_exercise" in types
    assert "institutional_13g" in types
    assert all(o.get("source_event_id", "").startswith("sec_live_") for o in out)
    assert stats["form4_filings_seen"] == 1
    assert stats["form4_filings_parsed"] == 1
    assert stats["form4_filings_skipped"] == 0
    assert stats["form4_transactions_emitted"] == 3
    assert stats["institutional_events_emitted"] == 1


def test_iter_live_events_tracks_skipped_form4(monkeypatch):
    from ogree_alpha.adapters import sec_edgar

    ticker_map = {
        "0": {"cik_str": 1024, "ticker": "PR", "title": "Permian Resources Corporation"},
    }
    submissions = {
        "filings": {
            "recent": {
                "form": ["4"],
                "accessionNumber": ["0001024-26-000001"],
                "filingDate": ["2026-02-01"],
                "primaryDocument": ["doc1.xml"],
            }
        }
    }

    def _fake_http_get_json(url: str, *, user_agent: str, timeout_s: int = 20):
        if url == SEC_TICKER_MAP_URL:
            return ticker_map
        if url == SEC_SUBMISSIONS_URL_TEMPLATE.format(cik="0000001024"):
            return submissions
        return {}

    monkeypatch.setattr(sec_edgar, "_http_get_json", _fake_http_get_json)
    monkeypatch.setattr(sec_edgar, "_http_get_text", lambda *args, **kwargs: "")

    stats = {}
    out = list(
        sec_edgar.iter_live_events(
            user_agent="OGREE Test (test@example.com)",
            max_filings_per_company=5,
            timeout_s=1,
            stats=stats,
        )
    )
    assert out == []
    assert stats["form4_filings_seen"] == 1
    assert stats["form4_filings_parsed"] == 0
    assert stats["form4_filings_skipped"] == 1
    assert stats["form4_transactions_emitted"] == 0


def test_load_ticker_map_cached_per_run(monkeypatch):
    from ogree_alpha.adapters import sec_edgar

    calls = {"n": 0}

    def _fake_http_get_json(url: str, *, user_agent: str, timeout_s: int = 20):
        calls["n"] += 1
        return {"0": {"cik_str": 1024, "ticker": "PR"}}

    monkeypatch.setattr(sec_edgar, "_http_get_json", _fake_http_get_json)

    cache = {}
    m1 = _load_ticker_to_cik_map(user_agent="UA", timeout_s=1, run_cache=cache)
    m2 = _load_ticker_to_cik_map(user_agent="UA", timeout_s=1, run_cache=cache)

    assert m1 == {"PR": "0000001024"}
    assert m2 == {"PR": "0000001024"}
    assert calls["n"] == 1


def test_parse_dt_formats():
    dt = _parse_dt("2026-02-10T15:20:00Z")
    assert dt is not None and dt.year == 2026
    dt2 = _parse_dt("2026-02-10")
    assert dt2 is not None
    assert _parse_dt(None) is None


def test_fixture_loads_and_covers_required_event_types():
    events = list(iter_fixture_events())
    assert len(events) >= 10
    types_seen = {(e.get("payload_json") or {}).get("type") for e in events}
    expected = {
        "insider_buy",
        "insider_sell",
        "insider_option_exercise",
        "institutional_13g",
        "institutional_13f",
    }
    missing = expected - types_seen
    assert not missing, f"Missing SEC fixture event types: {missing}"


def test_chain_scoring_adds_insider_buy_bonus_and_cluster_bonus():
    t0 = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "payload_json": {
                "lineage_id": "SEC:PERMIAN_RESOURCES",
                "company": "Permian Resources Corporation",
                "type": "insider_buy",
                "filer_name": "Dana Morgan",
            },
            "event_time": t0,
            "ingest_time": t0,
        },
        {
            "payload_json": {
                "lineage_id": "SEC:PERMIAN_RESOURCES",
                "company": "Permian Resources Corporation",
                "type": "insider_buy",
                "filer_name": "Ryan Cole",
            },
            "event_time": t0 + timedelta(days=14),
            "ingest_time": t0 + timedelta(days=14),
        },
    ]
    rows = compute_chain_scores(events)
    assert len(rows) == 1
    row = rows[0]
    assert row["has_insider_buy"] is True
    assert row["has_insider_buy_cluster"] is True
    assert row["score"] == 0.25


def test_chain_scoring_cluster_requires_distinct_insiders():
    t0 = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "payload_json": {
                "lineage_id": "SEC:MAGNOLIA_OIL_GAS",
                "company": "Magnolia Oil & Gas Corporation",
                "type": "insider_buy",
                "filer_name": "Sarah Patel",
            },
            "event_time": t0,
            "ingest_time": t0,
        },
        {
            "payload_json": {
                "lineage_id": "SEC:MAGNOLIA_OIL_GAS",
                "company": "Magnolia Oil & Gas Corporation",
                "type": "insider_buy",
                "filer_name": "Sarah Patel",
            },
            "event_time": t0 + timedelta(days=10),
            "ingest_time": t0 + timedelta(days=10),
        },
    ]
    rows = compute_chain_scores(events)
    assert rows[0]["has_insider_buy"] is True
    assert rows[0]["has_insider_buy_cluster"] is False
    assert rows[0]["score"] == 0.15


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set")
def test_sec_fixture_ingest_counts():
    from ogree_alpha.adapters.sec_edgar import ingest_fixture_to_db

    inserted, processed = ingest_fixture_to_db()
    assert processed >= 10
    inserted2, _ = ingest_fixture_to_db()
    assert inserted2 == 0
