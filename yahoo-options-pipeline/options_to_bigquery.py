"""Download Yahoo Finance option chains and append them to BigQuery.

Each run takes one snapshot: for every ticker, every listed expiration,
calls and puts, with all the contract fields yfinance returns plus the
underlying quote at download time.

Configuration (environment variables):
    GCP_PROJECT_ID   BigQuery project (default: prueba-server-507503)
    BQ_DATASET       dataset (default: yahoo_finance)
    BQ_TABLE         table (default: options_chain)
    BQ_LOCATION      dataset location (default: US)
    TICKERS          comma-separated list (default: SOXL,SPXL,TQQQ,TECL,NVDL,AAPL)
    DRY_RUN          "1" to skip BigQuery and write a CSV instead

Usage:
    python options_to_bigquery.py
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

DEFAULT_TICKERS = "SOXL,SPXL,TQQQ,TECL,NVDL,AAPL"
MAX_ATTEMPTS = 4
PAUSE_BETWEEN_REQUESTS = 0.5  # seconds, to stay polite with Yahoo

log = logging.getLogger("options_to_bigquery")

# Column name in the yfinance DataFrame -> column name in BigQuery.
CONTRACT_COLUMNS = {
    "contractSymbol": "contract_symbol",
    "lastTradeDate": "last_trade_date",
    "strike": "strike",
    "lastPrice": "last_price",
    "bid": "bid",
    "ask": "ask",
    "change": "change",
    "percentChange": "percent_change",
    "volume": "volume",
    "openInterest": "open_interest",
    "impliedVolatility": "implied_volatility",
    "inTheMoney": "in_the_money",
    "contractSize": "contract_size",
    "currency": "currency",
}

# (name, BigQuery type, mode)
SCHEMA = [
    ("snapshot_ts", "TIMESTAMP", "REQUIRED"),
    ("snapshot_date", "DATE", "REQUIRED"),
    ("ticker", "STRING", "REQUIRED"),
    ("option_type", "STRING", "REQUIRED"),
    ("expiration", "DATE", "REQUIRED"),
    ("days_to_expiration", "INT64", "NULLABLE"),
    ("contract_symbol", "STRING", "NULLABLE"),
    ("last_trade_date", "TIMESTAMP", "NULLABLE"),
    ("strike", "FLOAT64", "NULLABLE"),
    ("last_price", "FLOAT64", "NULLABLE"),
    ("bid", "FLOAT64", "NULLABLE"),
    ("ask", "FLOAT64", "NULLABLE"),
    ("change", "FLOAT64", "NULLABLE"),
    ("percent_change", "FLOAT64", "NULLABLE"),
    ("volume", "INT64", "NULLABLE"),
    ("open_interest", "INT64", "NULLABLE"),
    ("implied_volatility", "FLOAT64", "NULLABLE"),
    ("in_the_money", "BOOL", "NULLABLE"),
    ("contract_size", "STRING", "NULLABLE"),
    ("currency", "STRING", "NULLABLE"),
    ("underlying_price", "FLOAT64", "NULLABLE"),
    ("underlying_bid", "FLOAT64", "NULLABLE"),
    ("underlying_ask", "FLOAT64", "NULLABLE"),
    ("underlying_market_state", "STRING", "NULLABLE"),
    ("underlying_quote_json", "STRING", "NULLABLE"),
]


def with_retries(fn, what):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as exc:  # yfinance raises a variety of errors
            if attempt == MAX_ATTEMPTS:
                raise
            wait = 2 ** attempt
            log.warning("%s failed (%s); retry %d in %ds", what, exc, attempt, wait)
            time.sleep(wait)


def fetch_ticker(symbol):
    """Return one DataFrame with every contract of every expiration of `symbol`."""
    ticker = yf.Ticker(symbol)
    expirations = with_retries(lambda: ticker.options, f"{symbol} expirations")
    if not expirations:
        log.warning("%s: Yahoo returned no expirations", symbol)
        return pd.DataFrame()

    frames = []
    for expiration in expirations:
        chain = with_retries(lambda: ticker.option_chain(expiration),
                             f"{symbol} {expiration}")
        time.sleep(PAUSE_BETWEEN_REQUESTS)
        if chain.calls is None:
            continue
        underlying = chain.underlying or {}
        for option_type, df in (("call", chain.calls), ("put", chain.puts)):
            if df is None or df.empty:
                continue
            df = df.rename(columns=CONTRACT_COLUMNS)
            df["ticker"] = symbol
            df["option_type"] = option_type
            df["expiration"] = expiration
            df["underlying_price"] = underlying.get("regularMarketPrice")
            df["underlying_bid"] = underlying.get("bid")
            df["underlying_ask"] = underlying.get("ask")
            df["underlying_market_state"] = underlying.get("marketState")
            df["underlying_quote_json"] = json.dumps(underlying, default=str)
            frames.append(df)

    log.info("%s: %d expirations, %d contracts", symbol, len(expirations),
             sum(len(f) for f in frames))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def to_bigquery_frame(df, snapshot_ts):
    """Cast columns to the types in SCHEMA, in SCHEMA order."""
    df = df.copy()
    df["snapshot_ts"] = pd.Timestamp(snapshot_ts)
    df["snapshot_date"] = snapshot_ts.date()
    df["expiration"] = pd.to_datetime(df["expiration"]).dt.date
    df["days_to_expiration"] = (
        pd.to_datetime(df["expiration"]) - pd.Timestamp(snapshot_ts.date())
    ).dt.days.astype("Int64")
    df["last_trade_date"] = pd.to_datetime(df["last_trade_date"], utc=True)
    for col in ("volume", "open_interest"):
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")
    for name, bq_type, _ in SCHEMA:
        if name not in df:
            df[name] = None
        if bq_type == "FLOAT64":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("float64")
        elif bq_type == "BOOL":
            df[name] = df[name].astype("boolean")
        elif bq_type == "STRING":
            df[name] = df[name].astype("string")
    return df[[name for name, _, _ in SCHEMA]]


def load_to_bigquery(df, project, dataset, table, location):
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = bigquery.Table(
        f"{project}.{dataset}.{table}",
        schema=[bigquery.SchemaField(n, t, mode=m) for n, t, m in SCHEMA],
    )
    table_ref.time_partitioning = bigquery.TimePartitioning(field="snapshot_date")
    table_ref.clustering_fields = ["ticker", "option_type", "expiration"]
    client.create_table(table_ref, exists_ok=True)

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            schema=table_ref.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    log.info("Loaded %d rows into %s", job.output_rows, table_ref.full_table_id)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    project = os.environ.get("GCP_PROJECT_ID", "prueba-server-507503")
    dataset = os.environ.get("BQ_DATASET", "yahoo_finance")
    table = os.environ.get("BQ_TABLE", "options_chain")
    location = os.environ.get("BQ_LOCATION", "US")
    tickers = [t.strip().upper()
               for t in os.environ.get("TICKERS", DEFAULT_TICKERS).split(",")
               if t.strip()]
    dry_run = os.environ.get("DRY_RUN") == "1"

    snapshot_ts = datetime.now(timezone.utc).replace(microsecond=0)
    frames, failed = [], []
    for symbol in tickers:
        try:
            frames.append(fetch_ticker(symbol))
        except Exception:
            log.exception("%s: download failed", symbol)
            failed.append(symbol)

    frames = [f for f in frames if not f.empty]
    if frames:
        df = to_bigquery_frame(pd.concat(frames, ignore_index=True), snapshot_ts)
        if dry_run:
            path = f"options_{snapshot_ts:%Y%m%dT%H%M%SZ}.csv"
            df.to_csv(path, index=False)
            log.info("DRY_RUN: wrote %d rows to %s", len(df), path)
        else:
            load_to_bigquery(df, project, dataset, table, location)
    else:
        log.error("No option data downloaded")

    if failed or not frames:
        log.error("Tickers that failed: %s", ", ".join(failed) or "(none)")
        sys.exit(1)


if __name__ == "__main__":
    main()
