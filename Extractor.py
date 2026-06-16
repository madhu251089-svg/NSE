
#!/usr/bin/env python3
"""Fetch the NSE index option chain via the public JSON API and store a snapshot.

No browser (no Selenium): hits NSE's JSON API directly with ``requests``, so it
installs and runs in seconds -- fine for a GitHub Action, an old phone (Termux),
or a Raspberry Pi.

NSE retired the old ``/api/option-chain-indices`` endpoint; the current one is
``/api/option-chain-v3``, which requires an ``expiry`` parameter. So each run:
  1. warms up cookies on the option-chain page,
  2. looks up the expiry list via ``/api/option-chain-contract-info``,
  3. fetches ``/api/option-chain-v3`` for the nearest expiry.

Each run appends one snapshot (all strikes of that expiry) to the
``option_chain_snapshots`` table, using the exact column names the
rebuild/analysis scripts expect.

Dependencies: only ``requests`` (everything else is the standard library).
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Optional

import requests

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration constants
# --------------------------------------------------------------------------- #
DB_NAME: str = "nse_options_data.db"
SNAPSHOT_TABLE: str = "option_chain_snapshots"

NSE_BASE_URL: str = "https://www.nseindia.com"
OPTION_CHAIN_PAGE: str = "https://www.nseindia.com/option-chain"
CONTRACT_INFO_URL: str = "https://www.nseindia.com/api/option-chain-contract-info"
API_URL: str = "https://www.nseindia.com/api/option-chain-v3"
TARGET_SYMBOL: str = "NIFTY"          # NIFTY, BANKNIFTY, FINNIFTY, ...
INSTRUMENT_TYPE: str = "Indices"      # "Indices" for index options, "Equity" for stocks

REQUEST_TIMEOUT: int = 10             # seconds per HTTP request
MAX_RETRIES: int = 3                  # attempts before giving up
RETRY_BACKOFF: int = 2                # seconds, multiplied by the attempt number

PRICE_DECIMALS: int = 2               # rounding precision for prices
STRIKES_EACH_SIDE: int = 10           # strikes shown around ATM in the console preview

# NSE trades Mon-Fri, 09:15-15:30 IST. IST has no DST, so a fixed offset is exact.
IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

# Browser-like headers; NSE's edge rejects requests that do not look like a real
# browser session.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": OPTION_CHAIN_PAGE,
    "Connection": "keep-alive",
}

# Canonical snapshot schema. The rebuild/analysis scripts rely on Timestamp,
# Underlying_Price, Strike_Price, Call_OI and Put_OI; the rest are useful extras.
SNAPSHOT_COLUMNS = (
    "Timestamp",
    "Underlying_Price",
    "Strike_Price",
    "Call_OI",
    "Call_Chng_OI",
    "Call_LTP",
    "Put_LTP",
    "Put_Chng_OI",
    "Put_OI",
)
SNAPSHOT_DDL = f"""
CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
    Timestamp        TEXT,
    Underlying_Price REAL,
    Strike_Price     REAL,
    Call_OI          INTEGER,
    Call_Chng_OI     INTEGER,
    Call_LTP         REAL,
    Put_LTP          REAL,
    Put_Chng_OI      INTEGER,
    Put_OI           INTEGER
)
"""


@dataclass
class OptionChainData:
    """Parsed option-chain snapshot for a single expiry."""

    symbol: str
    underlying_price: float
    expiry: Optional[str]
    rows: list[dict] = field(default_factory=list)  # one dict per strike


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _to_int(value: object) -> int:
    """Best-effort conversion to int; returns 0 for missing / unparseable values."""
    try:
        return int(round(float(value)))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _to_price(value: object) -> float:
    """Best-effort conversion to a rounded float; returns 0.0 on failure."""
    try:
        return round(float(value), PRICE_DECIMALS)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def is_market_open(now: Optional[datetime] = None) -> bool:
    """Return ``True`` only during NSE trading hours: Mon-Fri, 09:15-15:30 IST.

    Trading holidays are not handled here -- add a holiday-date check if needed.
    """
    now = now or datetime.now(IST)
    if now.weekday() >= 5:          # 5 = Saturday, 6 = Sunday
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE


def _find_expiry_dates(obj: object) -> Optional[list]:
    """Recursively locate the first ``expiryDates`` list anywhere in a JSON blob.

    Robust to NSE nesting the field at the top level or under ``records``/``data``.
    """
    if isinstance(obj, dict):
        value = obj.get("expiryDates")
        if isinstance(value, list) and value:
            return value
        for nested in obj.values():
            found = _find_expiry_dates(nested)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_expiry_dates(item)
            if found:
                return found
    return None


def _nearest_expiry(expiries: list) -> Optional[str]:
    """Pick the soonest expiry that is today or later (NSE format: ``DD-Mon-YYYY``)."""
    today = datetime.now(IST).date()
    parsed = []
    for raw in expiries:
        try:
            parsed.append((datetime.strptime(raw, "%d-%b-%Y").date(), raw))
        except (TypeError, ValueError):
            continue
    if not parsed:
        return expiries[0] if expiries else None
    future = [pair for pair in parsed if pair[0] >= today]
    chosen = min(future) if future else min(parsed)
    return chosen[1]


# --------------------------------------------------------------------------- #
# Network + parsing
# --------------------------------------------------------------------------- #
def fetch_option_chain(
    symbol: str = TARGET_SYMBOL, instrument_type: str = INSTRUMENT_TYPE
) -> tuple[Optional[dict], Optional[str]]:
    """Fetch the raw option-chain JSON for ``symbol``'s nearest expiry.

    Returns ``(payload, expiry)``; ``(None, None)`` if every attempt fails.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # NSE sets anti-bot cookies on the home / option-chain pages; the API
            # calls only succeed once the session is carrying them.
            session.get(NSE_BASE_URL, timeout=REQUEST_TIMEOUT)
            session.get(OPTION_CHAIN_PAGE, timeout=REQUEST_TIMEOUT)

            # 1) Expiry list for this symbol.
            info = session.get(
                CONTRACT_INFO_URL, params={"symbol": symbol}, timeout=REQUEST_TIMEOUT
            )
            info.raise_for_status()
            expiries = _find_expiry_dates(info.json())
            if not expiries:
                raise ValueError("contract-info response contained no expiry dates")
            expiry = _nearest_expiry(expiries)

            # 2) Option chain for the nearest expiry (v3 requires the expiry param).
            response = session.get(
                API_URL,
                params={"type": instrument_type, "symbol": symbol, "expiry": expiry},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            return response.json(), expiry
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logger.warning("Fetch attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            time.sleep(RETRY_BACKOFF * attempt)

    logger.error("Could not fetch option chain after %d attempts: %s", MAX_RETRIES, last_error)
    return None, None


def _records_block(payload: dict) -> dict:
    """Return the dict that holds ``data``/``underlyingValue`` (nested or flat)."""
    records = payload.get("records")
    return records if isinstance(records, dict) else payload


def parse_option_chain(
    payload: dict, symbol: str = TARGET_SYMBOL, expiry: Optional[str] = None
) -> Optional[OptionChainData]:
    """Turn the raw JSON into an :class:`OptionChainData` for one expiry."""
    records = _records_block(payload)
    data = records.get("data") or payload.get("data") or []
    underlying = records.get("underlyingValue")
    if underlying is None:
        underlying = payload.get("underlyingValue")
    if not data or not underlying:
        logger.error("Unexpected payload shape (missing 'data' or 'underlyingValue').")
        return None

    rows: list[dict] = []
    for entry in data:
        # v3 already returns a single expiry; filter defensively only if the row
        # carries an expiryDate that disagrees with the one we asked for.
        if expiry and entry.get("expiryDate") not in (None, expiry):
            continue
        call = entry.get("CE") or {}
        put = entry.get("PE") or {}
        rows.append(
            {
                "Strike_Price": _to_int(entry.get("strikePrice")),
                "Call_OI": _to_int(call.get("openInterest")),
                "Call_Chng_OI": _to_int(call.get("changeinOpenInterest")),
                "Call_LTP": _to_price(call.get("lastPrice")),
                "Put_LTP": _to_price(put.get("lastPrice")),
                "Put_Chng_OI": _to_int(put.get("changeinOpenInterest")),
                "Put_OI": _to_int(put.get("openInterest")),
            }
        )

    if not rows:
        logger.error("No strikes found for expiry %s.", expiry)
        return None

    rows.sort(key=lambda r: r["Strike_Price"])
    return OptionChainData(
        symbol=symbol,
        underlying_price=_to_price(underlying),
        expiry=expiry,
        rows=rows,
    )


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #
def save_snapshot(
    chain: OptionChainData, db_name: str = DB_NAME, when: Optional[datetime] = None
) -> int:
    """Append the snapshot to ``option_chain_snapshots``; returns rows written."""
    timestamp = (when or datetime.now(IST)).strftime("%Y-%m-%d %H:%M:%S")
    records = [
        (
            timestamp,
            chain.underlying_price,
            row["Strike_Price"],
            row["Call_OI"],
            row["Call_Chng_OI"],
            row["Call_LTP"],
            row["Put_LTP"],
            row["Put_Chng_OI"],
            row["Put_OI"],
        )
        for row in chain.rows
    ]

    placeholders = ",".join(["?"] * len(SNAPSHOT_COLUMNS))
    insert_sql = (
        f"INSERT INTO {SNAPSHOT_TABLE} ({', '.join(SNAPSHOT_COLUMNS)}) "
        f"VALUES ({placeholders})"
    )
    with sqlite3.connect(db_name, timeout=60) as conn:
        conn.execute(SNAPSHOT_DDL)
        conn.executemany(insert_sql, records)
        conn.commit()
    return len(records)


# --------------------------------------------------------------------------- #
# Console preview (optional, no dependencies)
# --------------------------------------------------------------------------- #
def _print_preview(chain: OptionChainData, step: int = STRIKES_EACH_SIDE) -> None:
    """Print a small table of strikes around ATM (purely informational)."""
    strikes = [row["Strike_Price"] for row in chain.rows]
    atm_index = min(range(len(strikes)), key=lambda i: abs(strikes[i] - chain.underlying_price))
    lo = max(0, atm_index - step)
    hi = min(len(chain.rows), atm_index + step + 1)

    print(f"\n{chain.symbol}  spot={chain.underlying_price}  expiry={chain.expiry}")
    print(f"ATM strike: {strikes[atm_index]:,}\n")
    header = f"{'Call OI':>12} {'Call Chg':>10} | {'STRIKE':^8} | {'Put Chg':>10} {'Put OI':>12}"
    print(header)
    print("-" * len(header))
    for row in chain.rows[lo:hi]:
        marker = " <ATM" if row["Strike_Price"] == strikes[atm_index] else ""
        print(
            f"{row['Call_OI']:>12,} {row['Call_Chng_OI']:>10,} | "
            f"{row['Strike_Price']:^8,} | "
            f"{row['Put_Chng_OI']:>10,} {row['Put_OI']:>12,}{marker}"
        )


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    """Fetch a snapshot and store it, respecting market hours unless overridden."""
    parser = argparse.ArgumentParser(description="Snapshot the NSE option chain via the JSON API.")
    parser.add_argument("--symbol", default=TARGET_SYMBOL, help="Index symbol (default: NIFTY).")
    parser.add_argument("--type", default=INSTRUMENT_TYPE, help="Indices or Equity (default: Indices).")
    parser.add_argument("--db", default=DB_NAME, help="SQLite database path.")
    parser.add_argument(
        "--ignore-market-hours",
        action="store_true",
        help="Fetch even when the market is closed (for testing).",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Fetch and preview only; do not write to the database.",
    )
    # parse_known_args (not parse_args) so the script also runs inside Jupyter,
    # which injects an unrelated "-f <kernel>.json" argument we can safely ignore.
    args, _ = parser.parse_known_args()

    if not args.ignore_market_hours and not is_market_open():
        logger.info(
            "Market is closed (%s IST). NSE trades Mon-Fri 09:15-15:30 IST -- nothing to do.",
            datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        )
        return

    payload, expiry = fetch_option_chain(args.symbol, args.type)
    if payload is None:
        raise SystemExit(1)  # non-zero so a CI run surfaces the failure

    chain = parse_option_chain(payload, symbol=args.symbol, expiry=expiry)
    if chain is None:
        raise SystemExit(1)

    if not args.no_save:
        written = save_snapshot(chain, args.db)
        logger.info(
            "Saved %d strikes for %s %s (spot %s) to %s",
            written,
            args.symbol,
            chain.expiry,
            chain.underlying_price,
            args.db,
        )

    _print_preview(chain)


if __name__ == "__main__":
    main()
