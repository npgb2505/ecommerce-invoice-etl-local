from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"


def render() -> Path:
    result = json.loads((ARTIFACTS / "run_summary.json").read_text(encoding="utf-8"))
    document = f"""<!doctype html><html><head><meta charset="utf-8"><style>
    body{{font-family:Inter,Arial,sans-serif;background:#f7f4ed;color:#18232d;margin:0;padding:42px}}
    .shell{{max-width:1120px;margin:auto}}.badge{{display:inline-block;background:#e7f6ee;color:#17764c;padding:7px 11px;border-radius:999px;font-size:11px;font-weight:750}}
    h1{{font-size:34px;margin:12px 0 5px}}.sub{{color:#6c7882;margin-bottom:25px}}
    .cards{{display:grid;grid-template-columns:repeat(5,1fr);gap:13px}}.card,.flowbox{{background:#fff;border:1px solid #e0dbd1;border-radius:14px;padding:18px}}
    .label{{font-size:10px;color:#6c7882;text-transform:uppercase;letter-spacing:.08em}}.value{{font-size:26px;font-weight:760;margin-top:9px}}
    .flowbox{{margin-top:14px}}h2{{font-size:16px;margin:0 0 15px}}.flow{{display:flex;align-items:center;gap:8px}}
    .node{{flex:1;background:#efe9dc;border-radius:10px;padding:16px;text-align:center;font-size:12px;font-weight:700}}.arrow{{color:#927a5b}}
    </style></head><body><div class="shell"><span class="badge">SUCCESS · IDEMPOTENT LOAD</span>
    <h1>E-commerce Invoice ETL Run</h1><div class="sub">Actual local execution evidence</div>
    <section class="cards">
    <div class="card"><div class="label">Input</div><div class="value">{result['input_rows']:,}</div></div>
    <div class="card"><div class="label">Accepted</div><div class="value">{result['accepted_rows']:,}</div></div>
    <div class="card"><div class="label">Rejected</div><div class="value">{result['rejected_rows']:,}</div></div>
    <div class="card"><div class="label">Cancellations</div><div class="value">{result['cancellation_rows']:,}</div></div>
    <div class="card"><div class="label">Warehouse rows</div><div class="value">{result['warehouse_rows']:,}</div></div>
    </section><div class="flowbox"><h2>Executed workflow</h2><div class="flow">
    <div class="node">Invoice CSV</div><span class="arrow">→</span><div class="node">Contract</div><span class="arrow">→</span>
    <div class="node">Quality rules</div><span class="arrow">→</span><div class="node">Parquet + quarantine</div><span class="arrow">→</span>
    <div class="node">PostgreSQL star schema</div><span class="arrow">→</span><div class="node">RFM marts</div></div></div>
    </div></body></html>"""
    output = ARTIFACTS / "pipeline-run.html"
    output.write_text(document, encoding="utf-8")
    print(f"Rendered {output}")
    return output


if __name__ == "__main__":
    render()

