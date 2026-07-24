from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data" / "raw" / "ecommerce_invoices.csv"
DEFAULT_DSN = "postgresql://ecommerce:ecommerce@localhost:5543/ecommerce"
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


def extract(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype={"InvoiceNo": "string", "StockCode": "string", "CustomerID": "string"})
    missing = sorted(EXPECTED - set(frame.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    return frame


def transform(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    work = frame.copy()
    work["InvoiceDate"] = pd.to_datetime(work["InvoiceDate"], errors="coerce", utc=True)
    work["Quantity"] = pd.to_numeric(work["Quantity"], errors="coerce")
    work["UnitPrice"] = pd.to_numeric(work["UnitPrice"], errors="coerce")
    work["InvoiceNo"] = work["InvoiceNo"].str.strip()
    work["StockCode"] = work["StockCode"].str.strip().str.upper()
    work["Description"] = work["Description"].fillna("Unknown product").str.strip()
    work.loc[work["Description"] == "", "Description"] = "Unknown product"
    work["CustomerID"] = work["CustomerID"].fillna("GUEST").str.replace(r"\.0$", "", regex=True)
    work["Country"] = work["Country"].fillna("Unknown").str.strip()

    invalid_date = work["InvoiceDate"].isna()
    invalid_key = work["InvoiceNo"].isna() | work["StockCode"].isna()
    invalid_quantity = work["Quantity"].isna() | work["Quantity"].eq(0)
    invalid_price = work["UnitPrice"].isna() | work["UnitPrice"].lt(0)
    reject_mask = invalid_date | invalid_key | invalid_quantity | invalid_price
    rejected = work.loc[reject_mask].copy()
    rejected["rejection_reason"] = "invalid_record"
    rejected.loc[invalid_date, "rejection_reason"] = "invalid_invoice_date"
    rejected.loc[invalid_key, "rejection_reason"] = "missing_business_key"
    rejected.loc[invalid_quantity, "rejection_reason"] = "zero_or_invalid_quantity"
    rejected.loc[invalid_price, "rejection_reason"] = "negative_or_invalid_price"

    clean = work.loc[~reject_mask].copy().reset_index(drop=True)
    clean["is_cancellation"] = clean["InvoiceNo"].str.startswith("C") | clean["Quantity"].lt(0)
    clean["line_revenue"] = (clean["Quantity"] * clean["UnitPrice"]).round(2)
    clean["invoice_date"] = clean["InvoiceDate"].dt.date
    clean["invoice_hour"] = clean["InvoiceDate"].dt.hour
    clean["processed_at"] = datetime.now(timezone.utc)
    clean["line_id"] = [
        hashlib.sha256(
            f"{row.InvoiceNo}|{row.StockCode}|{row.InvoiceDate.isoformat()}|{index}".encode()
        ).hexdigest()[:24]
        for index, row in clean.iterrows()
    ]
    summary = {
        "input_rows": int(len(frame)),
        "accepted_rows": int(len(clean)),
        "rejected_rows": int(len(rejected)),
        "cancellation_rows": int(clean["is_cancellation"].sum()),
        "guest_rows": int(clean["CustomerID"].eq("GUEST").sum()),
    }
    return clean, rejected, summary


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
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ecommerce.pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    run_at TIMESTAMPTZ NOT NULL,
    input_rows INTEGER NOT NULL,
    accepted_rows INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    cancellation_rows INTEGER NOT NULL,
    status TEXT NOT NULL
);

CREATE OR REPLACE VIEW ecommerce.mart_daily_sales AS
SELECT invoice_date,
       ROUND(SUM(line_revenue), 2) AS net_revenue,
       COUNT(DISTINCT invoice_no) FILTER (WHERE NOT is_cancellation) AS invoices,
       COUNT(*) FILTER (WHERE is_cancellation) AS cancellation_lines
FROM ecommerce.fact_invoice_line
GROUP BY invoice_date;

CREATE OR REPLACE VIEW ecommerce.mart_country_sales AS
SELECT country,
       ROUND(SUM(line_revenue), 2) AS net_revenue,
       COUNT(DISTINCT customer_id) AS customers,
       COUNT(DISTINCT invoice_no) AS invoices
FROM ecommerce.fact_invoice_line
GROUP BY country;

CREATE OR REPLACE VIEW ecommerce.mart_customer_rfm AS
SELECT customer_id,
       MAX(invoice_date) AS last_purchase_date,
       COUNT(DISTINCT invoice_no) FILTER (WHERE NOT is_cancellation) AS frequency,
       ROUND(SUM(line_revenue), 2) AS monetary
FROM ecommerce.fact_invoice_line
WHERE customer_id <> 'GUEST'
GROUP BY customer_id;
"""


def load(clean: pd.DataFrame, dsn: str, summary: dict) -> dict:
    customer_rows = [
        (str(row.CustomerID), str(row.Country), row.processed_at.to_pydatetime() if hasattr(row.processed_at, "to_pydatetime") else row.processed_at)
        for row in clean.sort_values("InvoiceDate").drop_duplicates("CustomerID", keep="last").itertuples()
    ]
    product_rows = [
        (
            str(row.StockCode),
            str(row.Description),
            float(row.UnitPrice),
            row.processed_at.to_pydatetime() if hasattr(row.processed_at, "to_pydatetime") else row.processed_at,
        )
        for row in clean.sort_values("InvoiceDate").drop_duplicates("StockCode", keep="last").itertuples()
    ]
    fact_rows = [
        (
            str(row.line_id),
            str(row.InvoiceNo),
            str(row.StockCode),
            str(row.CustomerID),
            row.InvoiceDate.to_pydatetime(),
            row.invoice_date,
            int(row.invoice_hour),
            int(row.Quantity),
            float(row.UnitPrice),
            float(row.line_revenue),
            bool(row.is_cancellation),
            str(row.Country),
            row.processed_at.to_pydatetime() if hasattr(row.processed_at, "to_pydatetime") else row.processed_at,
        )
        for row in clean.itertuples()
    ]

    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(DDL)
            cursor.executemany(
                """
                INSERT INTO ecommerce.dim_customer (customer_id, country, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE
                SET country=EXCLUDED.country, updated_at=EXCLUDED.updated_at
                """,
                customer_rows,
            )
            cursor.executemany(
                """
                INSERT INTO ecommerce.dim_product
                    (stock_code, description, latest_unit_price, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (stock_code) DO UPDATE
                SET description=EXCLUDED.description,
                    latest_unit_price=EXCLUDED.latest_unit_price,
                    updated_at=EXCLUDED.updated_at
                """,
                product_rows,
            )
            cursor.executemany(
                """
                INSERT INTO ecommerce.fact_invoice_line
                    (line_id, invoice_no, stock_code, customer_id, invoice_timestamp,
                     invoice_date, invoice_hour, quantity, unit_price, line_revenue,
                     is_cancellation, country, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (line_id) DO UPDATE
                SET quantity=EXCLUDED.quantity,
                    unit_price=EXCLUDED.unit_price,
                    line_revenue=EXCLUDED.line_revenue,
                    is_cancellation=EXCLUDED.is_cancellation,
                    processed_at=EXCLUDED.processed_at
                """,
                fact_rows,
            )
            cursor.execute(
                """
                INSERT INTO ecommerce.pipeline_runs
                    (run_at, input_rows, accepted_rows, rejected_rows, cancellation_rows, status)
                VALUES (%s, %s, %s, %s, %s, 'success')
                """,
                (
                    datetime.now(timezone.utc),
                    summary["input_rows"],
                    summary["accepted_rows"],
                    summary["rejected_rows"],
                    summary["cancellation_rows"],
                ),
            )
            cursor.execute(
                """
                SELECT COUNT(*) AS warehouse_rows,
                       ROUND(SUM(line_revenue), 2) AS net_revenue,
                       COUNT(DISTINCT customer_id) AS customers
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
    countries = query_rows(
        dsn,
        "SELECT * FROM ecommerce.mart_country_sales ORDER BY net_revenue DESC",
    )
    products = query_rows(
        dsn,
        """
        SELECT p.description, ROUND(SUM(f.line_revenue), 2) AS revenue
        FROM ecommerce.fact_invoice_line f
        JOIN ecommerce.dim_product p USING (stock_code)
        GROUP BY p.description ORDER BY revenue DESC LIMIT 6
        """,
    )

    max_country = max((float(row["net_revenue"]) for row in countries), default=1)
    bars = "".join(
        f"""
        <div class="bar-row"><span>{html.escape(str(row['country']))}</span>
        <div class="track"><i style="width:{max(2, 100 * float(row['net_revenue']) / max_country):.1f}%"></i></div>
        <b>£{float(row['net_revenue']):,.0f}</b></div>
        """
        for row in countries
    )
    product_rows = "".join(
        f"<tr><td>{html.escape(str(row['description']))}</td><td>£{float(row['revenue']):,.2f}</td></tr>"
        for row in products
    )
    document = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>E-commerce Invoice ETL Demo</title>
<style>
body{{font-family:Inter,Arial,sans-serif;background:#f5f2ea;color:#17212b;margin:0;padding:42px}}
.shell{{max-width:1180px;margin:auto}} h1{{font-size:34px;margin:0}} .sub{{color:#667085;margin:8px 0 28px}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px}} .card,.panel{{background:white;border:1px solid #ded9ce;border-radius:14px;padding:20px}}
.value{{font-size:28px;font-weight:750;margin-top:10px}} .label{{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:#667085}}
.grid{{display:grid;grid-template-columns:1.35fr 1fr;gap:16px;margin-top:16px}} h2{{font-size:17px;margin:0 0 18px}}
.bar-row{{display:grid;grid-template-columns:120px 1fr 90px;gap:12px;align-items:center;margin:15px 0;font-size:13px}}
.track{{height:10px;background:#eee9df;border-radius:10px;overflow:hidden}} .track i{{display:block;height:100%;background:#e45b37;border-radius:10px}}
table{{width:100%;border-collapse:collapse;font-size:13px}} td{{padding:11px 0;border-bottom:1px solid #eee9df}} td:last-child{{text-align:right;font-weight:700}}
.ok{{display:inline-block;background:#e8f6ef;color:#16794b;padding:7px 11px;border-radius:999px;font-weight:700;font-size:12px}}
</style></head><body><div class="shell">
<span class="ok">PIPELINE RUN SUCCESSFUL</span><h1>E-commerce Invoice Warehouse</h1>
<div class="sub">Actual output generated by the local ETL demo</div>
<section class="cards">
<div class="card"><div class="label">Accepted rows</div><div class="value">{result['accepted_rows']:,}</div></div>
<div class="card"><div class="label">Rejected rows</div><div class="value">{result['rejected_rows']:,}</div></div>
<div class="card"><div class="label">Customers</div><div class="value">{int(result['customers']):,}</div></div>
<div class="card"><div class="label">Net revenue</div><div class="value">£{float(result['net_revenue']):,.0f}</div></div>
</section>
<section class="grid"><div class="panel"><h2>Net revenue by country</h2>{bars}</div>
<div class="panel"><h2>Top products</h2><table>{product_rows}</table></div></section>
</div></body></html>"""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(document, encoding="utf-8")


def run(input_path: Path, output_dir: Path, dsn: str) -> dict:
    started_at = datetime.now(timezone.utc)
    source = extract(input_path)
    clean, rejected, summary = transform(source)
    (output_dir / "clean").mkdir(parents=True, exist_ok=True)
    (output_dir / "rejected").mkdir(parents=True, exist_ok=True)
    clean_path = output_dir / "clean" / "invoice_lines.parquet"
    rejected_path = output_dir / "rejected" / "invoice_lines.csv"
    clean.to_parquet(clean_path, index=False)
    rejected.to_csv(rejected_path, index=False)
    metrics = load(clean, dsn, summary)
    result = {
        "status": "success",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        **summary,
        **metrics,
        "clean_output": str(clean_path),
        "rejected_output": str(rejected_path),
    }
    artifact_dir = ROOT / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    (artifact_dir / "run_summary.json").write_text(
        json.dumps(result, indent=2, default=str), encoding="utf-8"
    )
    render_dashboard(dsn, result, artifact_dir / "dashboard.html")
    print(json.dumps(result, indent=2, default=str))
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local e-commerce invoice ETL")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--dsn", default=os.getenv("WAREHOUSE_DSN", DEFAULT_DSN))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.output_dir, args.dsn)

