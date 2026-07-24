from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data" / "raw" / "Online Retail.xlsx"
DEFAULT_DSN = "postgresql://ecommerce:ecommerce@localhost:5543/ecommerce"
PIPELINE_NAME = "uci_online_retail_etl"
EXPECTED = {
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
}


CONTROL_DDL = """
CREATE SCHEMA IF NOT EXISTS ecommerce;
CREATE TABLE IF NOT EXISTS ecommerce.pipeline_watermarks (
    pipeline_name TEXT PRIMARY KEY,
    last_event_at TIMESTAMPTZ,
    last_batch_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);
"""


DDL = """
CREATE SCHEMA IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.dim_customer (
    customer_id TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ecommerce.dim_product (
    stock_code TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    latest_unit_price NUMERIC(12,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ecommerce.fact_invoice_line (
    line_id TEXT PRIMARY KEY,
    invoice_no TEXT NOT NULL,
    stock_code TEXT NOT NULL REFERENCES ecommerce.dim_product(stock_code),
    customer_id TEXT NOT NULL REFERENCES ecommerce.dim_customer(customer_id),
    invoice_timestamp TIMESTAMPTZ NOT NULL,
    invoice_date DATE NOT NULL,
    invoice_hour INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    line_revenue NUMERIC(14,2) NOT NULL,
    is_cancellation BOOLEAN NOT NULL,
    country TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS fact_invoice_line_date_idx
    ON ecommerce.fact_invoice_line (invoice_date);
CREATE INDEX IF NOT EXISTS fact_invoice_line_customer_idx
    ON ecommerce.fact_invoice_line (customer_id);
CREATE INDEX IF NOT EXISTS fact_invoice_line_stock_idx
    ON ecommerce.fact_invoice_line (stock_code);

CREATE TABLE IF NOT EXISTS ecommerce.pipeline_runs (
    batch_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    run_mode TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    source_rows INTEGER NOT NULL,
    window_rows INTEGER NOT NULL,
    accepted_rows INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    cancellation_rows INTEGER NOT NULL,
    rejection_rate NUMERIC(10,6) NOT NULL,
    start_at TIMESTAMPTZ,
    end_at TIMESTAMPTZ,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ecommerce.data_quality_results (
    batch_id TEXT NOT NULL REFERENCES ecommerce.pipeline_runs(batch_id),
    check_name TEXT NOT NULL,
    check_value NUMERIC(18,6) NOT NULL,
    threshold NUMERIC(18,6),
    passed BOOLEAN NOT NULL,
    PRIMARY KEY (batch_id, check_name)
);

CREATE TABLE IF NOT EXISTS ecommerce.pipeline_watermarks (
    pipeline_name TEXT PRIMARY KEY,
    last_event_at TIMESTAMPTZ,
    last_batch_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE OR REPLACE VIEW ecommerce.mart_daily_sales AS
SELECT invoice_date,
       ROUND(SUM(line_revenue), 2) AS net_revenue,
       COUNT(DISTINCT invoice_no) FILTER (WHERE NOT is_cancellation) AS invoices,
       COUNT(*) FILTER (WHERE is_cancellation) AS cancellation_lines,
       COUNT(DISTINCT customer_id) FILTER (WHERE customer_id <> 'GUEST') AS known_customers
FROM ecommerce.fact_invoice_line
GROUP BY invoice_date;

CREATE OR REPLACE VIEW ecommerce.mart_country_sales AS
SELECT country,
       ROUND(SUM(line_revenue), 2) AS net_revenue,
       COUNT(DISTINCT customer_id) FILTER (WHERE customer_id <> 'GUEST') AS customers,
       COUNT(DISTINCT invoice_no) AS invoices,
       ROUND(100.0 * AVG(is_cancellation::integer), 2) AS cancellation_line_rate
FROM ecommerce.fact_invoice_line
GROUP BY country;

CREATE OR REPLACE VIEW ecommerce.mart_customer_rfm AS
WITH anchor AS (
    SELECT MAX(invoice_date) + 1 AS analysis_date
    FROM ecommerce.fact_invoice_line
)
SELECT f.customer_id,
       (a.analysis_date - MAX(f.invoice_date))::integer AS recency_days,
       COUNT(DISTINCT f.invoice_no) FILTER (WHERE NOT f.is_cancellation) AS frequency,
       ROUND(SUM(f.line_revenue), 2) AS monetary
FROM ecommerce.fact_invoice_line f
CROSS JOIN anchor a
WHERE f.customer_id <> 'GUEST'
GROUP BY f.customer_id, a.analysis_date;

CREATE OR REPLACE VIEW ecommerce.mart_product_performance AS
SELECT f.stock_code,
       p.description,
       SUM(f.quantity) AS net_units,
       ROUND(SUM(f.line_revenue), 2) AS net_revenue,
       COUNT(DISTINCT f.invoice_no) AS invoice_count,
       COUNT(*) FILTER (WHERE f.is_cancellation) AS cancellation_lines
FROM ecommerce.fact_invoice_line f
JOIN ecommerce.dim_product p USING (stock_code)
GROUP BY f.stock_code, p.description;
"""


def extract(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist: {path}")
    if path.suffix.lower() in {".xlsx", ".xls"}:
        frame = pd.read_excel(path, engine="openpyxl")
    else:
        frame = pd.read_csv(path)
    missing = sorted(EXPECTED - set(frame.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    return frame


def get_watermark(dsn: str) -> datetime | None:
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(CONTROL_DDL)
            cursor.execute(
                "SELECT last_event_at FROM ecommerce.pipeline_watermarks WHERE pipeline_name=%s",
                (PIPELINE_NAME,),
            )
            row = cursor.fetchone()
        connection.commit()
    return row["last_event_at"] if row else None


def transform(
    frame: pd.DataFrame,
    batch_id: str = "test-batch",
    start_at: datetime | None = None,
    end_at: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    work = frame.copy()
    work["_source_row"] = work.index.astype("int64")
    work["InvoiceDate"] = pd.to_datetime(work["InvoiceDate"], errors="coerce", utc=True)
    work["Quantity"] = pd.to_numeric(work["Quantity"], errors="coerce")
    work["UnitPrice"] = pd.to_numeric(work["UnitPrice"], errors="coerce")
    work["InvoiceNo"] = work["InvoiceNo"].astype("string").str.strip()
    work["StockCode"] = work["StockCode"].astype("string").str.strip().str.upper()
    work["Description"] = work["Description"].fillna("Unknown product").astype(str).str.strip()
    work.loc[work["Description"] == "", "Description"] = "Unknown product"
    work["CustomerID"] = (
        work["CustomerID"].astype("string").fillna("GUEST").str.replace(r"\.0$", "", regex=True)
    )
    work["Country"] = work["Country"].fillna("Unknown").astype(str).str.strip()

    if start_at is not None:
        work = work.loc[work["InvoiceDate"] >= pd.Timestamp(start_at)]
    if end_at is not None:
        work = work.loc[work["InvoiceDate"] <= pd.Timestamp(end_at)]
    window_rows = len(work)

    invalid_date = work["InvoiceDate"].isna()
    invalid_key = (
        work["InvoiceNo"].isna()
        | work["InvoiceNo"].eq("")
        | work["StockCode"].isna()
        | work["StockCode"].eq("")
    )
    invalid_quantity = work["Quantity"].isna() | work["Quantity"].eq(0)
    invalid_price = work["UnitPrice"].isna() | work["UnitPrice"].lt(0)
    reject_mask = invalid_date | invalid_key | invalid_quantity | invalid_price
    rejected = work.loc[reject_mask].copy()
    rejected["rejection_reason"] = "invalid_record"
    rejected.loc[invalid_date, "rejection_reason"] = "invalid_invoice_date"
    rejected.loc[invalid_key, "rejection_reason"] = "missing_business_key"
    rejected.loc[invalid_quantity, "rejection_reason"] = "zero_or_invalid_quantity"
    rejected.loc[invalid_price, "rejection_reason"] = "negative_or_invalid_price"

    clean = work.loc[~reject_mask].copy()
    clean["Quantity"] = clean["Quantity"].astype("int64")
    clean["is_cancellation"] = clean["InvoiceNo"].str.startswith("C") | clean["Quantity"].lt(0)
    clean["line_revenue"] = (clean["Quantity"] * clean["UnitPrice"]).round(2)
    clean["invoice_date"] = clean["InvoiceDate"].dt.date
    clean["invoice_hour"] = clean["InvoiceDate"].dt.hour
    clean["batch_id"] = batch_id
    clean["processed_at"] = datetime.now(timezone.utc)
    identity_fields = [
        "InvoiceNo",
        "StockCode",
        "InvoiceDate",
        "Quantity",
        "UnitPrice",
        "CustomerID",
        "_source_row",
    ]
    signatures = clean[identity_fields].astype("string").fillna("<null>").agg("|".join, axis=1)
    clean["line_id"] = signatures.map(
        lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]
    )
    clean = clean.drop(columns=["_source_row"])

    rejected_rows = len(rejected)
    summary = {
        "input_rows": int(len(frame)),
        "source_rows": int(len(frame)),
        "window_rows": int(window_rows),
        "accepted_rows": int(len(clean)),
        "rejected_rows": int(rejected_rows),
        "rejection_rate": round(rejected_rows / max(window_rows, 1), 6),
        "cancellation_rows": int(clean["is_cancellation"].sum()),
        "guest_rows": int(clean["CustomerID"].eq("GUEST").sum()),
        "distinct_lines": int(clean["line_id"].nunique()),
        "min_invoice_at": clean["InvoiceDate"].min().isoformat() if len(clean) else None,
        "max_invoice_at": clean["InvoiceDate"].max().isoformat() if len(clean) else None,
    }
    return clean, rejected, summary


def _python_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        return value.item()
    return value


def load(
    clean: pd.DataFrame,
    dsn: str,
    summary: dict,
    batch_id: str,
    started_at: datetime,
    start_at: datetime | None,
    end_at: datetime | None,
    full_refresh: bool,
) -> dict:
    finished_at = datetime.now(timezone.utc)
    run_mode = "full_refresh" if full_refresh else "incremental"
    copy_columns = [
        "line_id",
        "InvoiceNo",
        "StockCode",
        "CustomerID",
        "InvoiceDate",
        "invoice_date",
        "invoice_hour",
        "Quantity",
        "UnitPrice",
        "line_revenue",
        "is_cancellation",
        "Country",
        "batch_id",
        "processed_at",
        "Description",
    ]

    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(DDL)
            cursor.execute(
                """
                CREATE TEMP TABLE stage_invoice_lines (
                    line_id TEXT, invoice_no TEXT, stock_code TEXT, customer_id TEXT,
                    invoice_timestamp TIMESTAMPTZ, invoice_date DATE, invoice_hour INTEGER,
                    quantity INTEGER, unit_price NUMERIC(12,2), line_revenue NUMERIC(14,2),
                    is_cancellation BOOLEAN, country TEXT, batch_id TEXT,
                    processed_at TIMESTAMPTZ, description TEXT
                ) ON COMMIT DROP
                """
            )
            with cursor.copy(
                """
                COPY stage_invoice_lines
                    (line_id, invoice_no, stock_code, customer_id, invoice_timestamp,
                     invoice_date, invoice_hour, quantity, unit_price, line_revenue,
                     is_cancellation, country, batch_id, processed_at, description)
                FROM STDIN
                """
            ) as copy:
                for row in clean[copy_columns].itertuples(index=False, name=None):
                    copy.write_row(tuple(_python_value(value) for value in row))

            if full_refresh:
                cursor.execute("TRUNCATE TABLE ecommerce.fact_invoice_line")

            cursor.execute(
                """
                INSERT INTO ecommerce.dim_customer (customer_id, country, updated_at)
                SELECT DISTINCT ON (customer_id) customer_id, country, processed_at
                FROM stage_invoice_lines
                ORDER BY customer_id, invoice_timestamp DESC
                ON CONFLICT (customer_id) DO UPDATE SET
                    country=EXCLUDED.country,
                    updated_at=EXCLUDED.updated_at
                """
            )
            cursor.execute(
                """
                INSERT INTO ecommerce.dim_product
                    (stock_code, description, latest_unit_price, updated_at)
                SELECT DISTINCT ON (stock_code)
                    stock_code, description, unit_price, processed_at
                FROM stage_invoice_lines
                ORDER BY stock_code, invoice_timestamp DESC
                ON CONFLICT (stock_code) DO UPDATE SET
                    description=EXCLUDED.description,
                    latest_unit_price=EXCLUDED.latest_unit_price,
                    updated_at=EXCLUDED.updated_at
                """
            )
            cursor.execute(
                """
                INSERT INTO ecommerce.fact_invoice_line
                    (line_id, invoice_no, stock_code, customer_id, invoice_timestamp,
                     invoice_date, invoice_hour, quantity, unit_price, line_revenue,
                     is_cancellation, country, batch_id, processed_at)
                SELECT line_id, invoice_no, stock_code, customer_id, invoice_timestamp,
                       invoice_date, invoice_hour, quantity, unit_price, line_revenue,
                       is_cancellation, country, batch_id, processed_at
                FROM stage_invoice_lines
                ON CONFLICT (line_id) DO UPDATE SET
                    quantity=EXCLUDED.quantity,
                    unit_price=EXCLUDED.unit_price,
                    line_revenue=EXCLUDED.line_revenue,
                    is_cancellation=EXCLUDED.is_cancellation,
                    country=EXCLUDED.country,
                    batch_id=EXCLUDED.batch_id,
                    processed_at=EXCLUDED.processed_at
                """
            )
            cursor.execute(
                """
                INSERT INTO ecommerce.pipeline_runs
                    (batch_id, pipeline_name, run_mode, started_at, finished_at,
                     source_rows, window_rows, accepted_rows, rejected_rows,
                     cancellation_rows, rejection_rate, start_at, end_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'success')
                ON CONFLICT (batch_id) DO NOTHING
                """,
                (
                    batch_id,
                    PIPELINE_NAME,
                    run_mode,
                    started_at,
                    finished_at,
                    summary["source_rows"],
                    summary["window_rows"],
                    summary["accepted_rows"],
                    summary["rejected_rows"],
                    summary["cancellation_rows"],
                    summary["rejection_rate"],
                    start_at,
                    end_at,
                ),
            )
            quality_results = [
                (
                    batch_id,
                    "rejection_rate_below_5_percent",
                    summary["rejection_rate"],
                    0.05,
                    summary["rejection_rate"] < 0.05,
                ),
                (
                    batch_id,
                    "accepted_rows_positive",
                    summary["accepted_rows"],
                    1,
                    summary["accepted_rows"] > 0,
                ),
                (
                    batch_id,
                    "line_ids_are_unique",
                    summary["distinct_lines"],
                    summary["accepted_rows"],
                    summary["distinct_lines"] == summary["accepted_rows"],
                ),
            ]
            cursor.executemany(
                """
                INSERT INTO ecommerce.data_quality_results
                    (batch_id, check_name, check_value, threshold, passed)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, check_name) DO UPDATE SET
                    check_value=EXCLUDED.check_value,
                    threshold=EXCLUDED.threshold,
                    passed=EXCLUDED.passed
                """,
                quality_results,
            )
            if not all(row[-1] for row in quality_results):
                raise ValueError("One or more post-transform data quality checks failed")

            max_event_at = clean["InvoiceDate"].max().to_pydatetime()
            cursor.execute(
                """
                INSERT INTO ecommerce.pipeline_watermarks
                    (pipeline_name, last_event_at, last_batch_id, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (pipeline_name) DO UPDATE SET
                    last_event_at=GREATEST(
                        ecommerce.pipeline_watermarks.last_event_at,
                        EXCLUDED.last_event_at
                    ),
                    last_batch_id=EXCLUDED.last_batch_id,
                    updated_at=EXCLUDED.updated_at
                """,
                (PIPELINE_NAME, max_event_at, batch_id, finished_at),
            )
            cursor.execute(
                """
                SELECT COUNT(*) AS warehouse_rows,
                       ROUND(SUM(line_revenue), 2) AS net_revenue,
                       COUNT(DISTINCT customer_id) FILTER (WHERE customer_id <> 'GUEST') AS customers,
                       MIN(invoice_timestamp) AS warehouse_min_at,
                       MAX(invoice_timestamp) AS warehouse_max_at
                FROM ecommerce.fact_invoice_line
                """
            )
            metrics = dict(cursor.fetchone())
        connection.commit()
    return metrics


def query_rows(dsn: str, statement: str) -> list[dict]:
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            return [dict(row) for row in cursor.fetchall()]


def render_dashboard(dsn: str, result: dict, output: Path) -> None:
    totals = query_rows(
        dsn,
        """
        SELECT COUNT(*) AS warehouse_rows,
               COUNT(*) FILTER (WHERE is_cancellation) AS cancellation_rows,
               COUNT(DISTINCT customer_id) FILTER (WHERE customer_id <> 'GUEST') AS customers,
               ROUND(SUM(line_revenue), 2) AS net_revenue
        FROM ecommerce.fact_invoice_line
        """,
    )[0]
    countries = query_rows(
        dsn,
        "SELECT * FROM ecommerce.mart_country_sales ORDER BY net_revenue DESC LIMIT 10",
    )
    products = query_rows(
        dsn,
        "SELECT * FROM ecommerce.mart_product_performance ORDER BY net_revenue DESC LIMIT 8",
    )
    max_country = max((float(row["net_revenue"]) for row in countries), default=1)
    bars = "".join(
        f"""<div class="bar-row"><span>{html.escape(str(row['country']))}</span>
        <div class="track"><i style="width:{max(2, 100 * float(row['net_revenue']) / max_country):.1f}%"></i></div>
        <b>£{float(row['net_revenue']):,.0f}</b></div>"""
        for row in countries
    )
    product_rows = "".join(
        f"<tr><td>{html.escape(str(row['description']))}</td><td>£{float(row['net_revenue']):,.0f}</td></tr>"
        for row in products
    )
    document = f"""<!doctype html><html><head><meta charset="utf-8"><title>Online Retail Warehouse</title>
<style>body{{font-family:Inter,Arial,sans-serif;background:#f5f2ea;color:#17212b;margin:0;padding:42px}}
.shell{{max-width:1180px;margin:auto}}h1{{font-size:34px;margin:8px 0}}.sub{{color:#667085;margin:0 0 28px}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px}}.card,.panel{{background:white;border:1px solid #ded9ce;border-radius:14px;padding:20px}}
.value{{font-size:28px;font-weight:750;margin-top:10px}}.label{{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:#667085}}
.grid{{display:grid;grid-template-columns:1.35fr 1fr;gap:16px;margin-top:16px}}h2{{font-size:17px;margin:0 0 18px}}
.bar-row{{display:grid;grid-template-columns:120px 1fr 90px;gap:12px;align-items:center;margin:15px 0;font-size:13px}}
.track{{height:10px;background:#eee9df;border-radius:10px;overflow:hidden}}.track i{{display:block;height:100%;background:#e45b37;border-radius:10px}}
table{{width:100%;border-collapse:collapse;font-size:13px}}td{{padding:11px 0;border-bottom:1px solid #eee9df}}td:last-child{{text-align:right;font-weight:700}}
.ok{{display:inline-block;background:#e8f6ef;color:#16794b;padding:7px 11px;border-radius:999px;font-weight:700;font-size:12px}}</style>
</head><body><div class="shell"><span class="ok">FULL DATASET · QUALITY GATE PASSED</span>
<h1>UCI Online Retail Warehouse</h1><div class="sub">Production-style result generated from the complete public workbook</div>
<section class="cards">
<div class="card"><div class="label">Warehouse lines</div><div class="value">{int(totals['warehouse_rows']):,}</div></div>
<div class="card"><div class="label">Cancellations</div><div class="value">{int(totals['cancellation_rows']):,}</div></div>
<div class="card"><div class="label">Known customers</div><div class="value">{int(totals['customers']):,}</div></div>
<div class="card"><div class="label">Net revenue</div><div class="value">£{float(totals['net_revenue']):,.0f}</div></div>
</section><section class="grid"><div class="panel"><h2>Net revenue by country</h2>{bars}</div>
<div class="panel"><h2>Top products</h2><table>{product_rows}</table></div></section>
</div></body></html>"""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(document, encoding="utf-8")


def write_observability(result: dict) -> None:
    artifacts = ROOT / "artifacts"
    runs = artifacts / "runs"
    artifacts.mkdir(exist_ok=True)
    runs.mkdir(exist_ok=True)
    payload = json.dumps(result, indent=2, default=str)
    (artifacts / "run_summary.json").write_text(payload, encoding="utf-8")
    (runs / f"{result['batch_id']}.json").write_text(payload, encoding="utf-8")
    metrics = [
        "# HELP ecommerce_etl_rows Rows handled by the latest run",
        "# TYPE ecommerce_etl_rows gauge",
        f'ecommerce_etl_rows{{state="source"}} {result["source_rows"]}',
        f'ecommerce_etl_rows{{state="accepted"}} {result["accepted_rows"]}',
        f'ecommerce_etl_rows{{state="rejected"}} {result["rejected_rows"]}',
        "# HELP ecommerce_etl_rejection_rate Rejected rows divided by processing-window rows",
        "# TYPE ecommerce_etl_rejection_rate gauge",
        f"ecommerce_etl_rejection_rate {result['rejection_rate']}",
        "# HELP ecommerce_warehouse_rows Invoice lines in the warehouse",
        "# TYPE ecommerce_warehouse_rows gauge",
        f"ecommerce_warehouse_rows {result['warehouse_rows']}",
    ]
    (artifacts / "metrics.prom").write_text("\n".join(metrics) + "\n", encoding="utf-8")


def run(
    input_path: Path,
    output_dir: Path,
    dsn: str,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    full_refresh: bool = False,
    lookback_days: int = 2,
    batch_id: str | None = None,
) -> dict:
    started_at = datetime.now(timezone.utc)
    batch_id = batch_id or uuid.uuid4().hex
    watermark = None if full_refresh else get_watermark(dsn)
    effective_start = start_at
    if effective_start is None and watermark is not None:
        effective_start = watermark - timedelta(days=lookback_days)
    source = extract(input_path)
    clean, rejected, summary = transform(source, batch_id, effective_start, end_at)
    if summary["window_rows"] == 0 or summary["accepted_rows"] == 0:
        raise ValueError("No valid rows fall inside the requested processing window")

    clean_dir = output_dir / "clean" / f"batch_id={batch_id}"
    rejected_dir = output_dir / "rejected" / f"batch_id={batch_id}"
    clean_dir.mkdir(parents=True, exist_ok=True)
    rejected_dir.mkdir(parents=True, exist_ok=True)
    clean_path = clean_dir / "invoice_lines.parquet"
    rejected_path = rejected_dir / "invoice_lines.csv"
    clean.to_parquet(clean_path, index=False)
    rejected.to_csv(rejected_path, index=False)

    warehouse = load(
        clean,
        dsn,
        summary,
        batch_id,
        started_at,
        effective_start,
        end_at,
        full_refresh,
    )
    result = {
        "status": "success",
        "pipeline_name": PIPELINE_NAME,
        "batch_id": batch_id,
        "run_mode": "full_refresh" if full_refresh else "incremental",
        "requested_start_at": start_at.isoformat() if start_at else None,
        "effective_start_at": effective_start.isoformat() if effective_start else None,
        "end_at": end_at.isoformat() if end_at else None,
        "watermark_before_run": watermark.isoformat() if watermark else None,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        **summary,
        **warehouse,
        "clean_output": str(clean_path),
        "rejected_output": str(rejected_path),
    }
    write_observability(result)
    render_dashboard(dsn, result, ROOT / "artifacts" / "dashboard.html")
    print(json.dumps(result, indent=2, default=str))
    return result


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full UCI Online Retail ETL")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--dsn", default=os.getenv("WAREHOUSE_DSN", DEFAULT_DSN))
    parser.add_argument("--start-at", type=parse_timestamp)
    parser.add_argument("--end-at", type=parse_timestamp)
    parser.add_argument("--lookback-days", type=int, default=2)
    parser.add_argument("--batch-id")
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run(
        arguments.input,
        arguments.output_dir,
        arguments.dsn,
        arguments.start_at,
        arguments.end_at,
        arguments.full_refresh,
        arguments.lookback_days,
        arguments.batch_id,
    )
