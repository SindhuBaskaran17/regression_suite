import os, glob, json
from datetime import datetime

import pandas as pd
from jinja2 import Template

from db import get_engine
from dq_checks import CHECK_DISPATCH, check_pk_not_null, check_pk_unique

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "config")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
TEMPLATE_FILE = os.path.join(BASE_DIR, "dq_html_template.html")

def load_config_files():
    for path in glob.glob(os.path.join(CONFIG_DIR, "*.json")):
        with open(path, "r", encoding="utf-8") as f:
            yield json.load(f)

def run_table(engine, cfg):
    table_name = cfg["table"]
    query = cfg.get("load", {}).get("query", f"SELECT * FROM {table_name}")
    df = pd.read_sql(query, engine)
    results = []

    for tcheck in cfg.get("table_checks", []):
        ctype = tcheck["type"]
        if ctype == "pk_not_null":
            res = check_pk_not_null(df, table_name, cfg.get("pk", []))
        elif ctype == "pk_unique":
            res = check_pk_unique(df, table_name, cfg.get("pk", []))
        else:
            level, fn = CHECK_DISPATCH[ctype]
            res = fn(df, table_name, tcheck)
        results.append(res)

    for col, colcfg in cfg.get("columns", {}).items():
        for c in colcfg.get("checks", []):
            ctype = c["type"]
            level, fn = CHECK_DISPATCH[ctype]
            res = fn(df, table_name, col, c) if level == "column" else fn(df, table_name, c)
            results.append(res)

    return results

def build_summary(all_results):
    tables = {}
    for r in all_results:
        tbl = r["table"]
        if tbl not in tables:
            tables[tbl] = {"name": tbl, "total": 0, "passed": 0, "failed": 0}
        tables[tbl]["total"] += 1
        if r["passed"]:
            tables[tbl]["passed"] += 1
        else:
            tables[tbl]["failed"] += 1

    total_checks = len(all_results)
    passed_checks = sum(1 for r in all_results if r["passed"])
    failed_checks = total_checks - passed_checks
    pass_rate = round((passed_checks / total_checks) * 100, 2) if total_checks else 0.0

    return {
        "run_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "tables": list(tables.values()),
        "total_tables": len(tables),
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "pass_rate": pass_rate,
        "checks": all_results,
    }

def render_html(summary):
    with open(TEMPLATE_FILE, "r", encoding="utf-8") as f:
        tpl = Template(f.read())
    return tpl.render(**summary)

def main():
    engine = get_engine()
    all_results = []
    for cfg in load_config_files():
        all_results.extend(run_table(engine, cfg))

    summary = build_summary(all_results)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    html = render_html(summary)
    out_file = os.path.join(OUTPUT_DIR, f"report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.html")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html)

    with open(os.path.join(OUTPUT_DIR, "last_run_details.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)


    print("DQ report created:", out_file)

if __name__ == "__main__":
    main()

