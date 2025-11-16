import re
import pandas as pd
from datetime import datetime, timezone

def _result(passed, check_name, level, table, column=None, details=None, failed_df=None):
    return {
        "passed": bool(passed),
        "check_name": check_name,
        "level": level,
        "table": table,
        "column": column,
        "details": details or "",
        "failed_sample": (
            failed_df.head(100).to_dict(orient="records")
            if isinstance(failed_df, pd.DataFrame) and not failed_df.empty else []
        ),
        "failed_count": 0 if failed_df is None else len(failed_df)
    }

def check_row_count_min(df, table, spec):
    min_rows = spec.get("min", 1)
    ok = len(df) >= min_rows
    return _result(ok, "row_count_min", "table", table, details=f"rows={len(df)}, min={min_rows}")

def check_row_count_between(df, table, spec):
    min_rows = spec.get("min", 0)
    max_rows = spec.get("max", 10**12)
    ok = (len(df) >= min_rows) and (len(df) <= max_rows)
    return _result(ok, "row_count_between", "table", table, details=f"rows={len(df)}, range=[{min_rows}, {max_rows}]")

def check_pk_not_null(df, table, pk_cols):
    mask = df[pk_cols].isnull().any(axis=1)
    failed = df[mask]
    ok = failed.empty
    return _result(ok, "pk_not_null", "table", table, failed_df=failed)

def check_pk_unique(df, table, pk_cols):
    dupes = df[df.duplicated(subset=pk_cols, keep=False)].sort_values(pk_cols)
    ok = dupes.empty
    return _result(ok, "pk_unique", "table", table, failed_df=dupes)

def check_not_null(df, table, col, spec):
    failed = df[df[col].isnull()]
    ok = failed.empty
    return _result(ok, "not_null", "column", table, column=col, failed_df=failed)

def check_in_set(df, table, col, spec):
    allowed = set(spec["values"])
    failed = df[~df[col].isin(allowed) & df[col].notnull()]
    ok = failed.empty
    return _result(ok, "in_set", "column", table, column=col, failed_df=failed, details=f"allowed={sorted(allowed)}")

def check_regex(df, table, col, spec):
    pattern = re.compile(spec["pattern"])
    mask = df[col].notnull() & ~df[col].astype(str).str.match(pattern)
    failed = df[mask]
    ok = failed.empty
    return _result(ok, "regex", "column", table, column=col, failed_df=failed, details=f"pattern={spec['pattern']}")

def check_length_between(df, table, col, spec):
    min_len = spec.get("min", 0)
    max_len = spec.get("max", 10**6)
    s = df[col].astype(str)
    mask = df[col].notnull() & ((s.str.len() < min_len) | (s.str.len() > max_len))
    failed = df[mask]
    ok = failed.empty
    return _result(ok, "length_between", "column", table, column=col, failed_df=failed, details=f"range=[{min_len},{max_len}]")

def check_numeric_min(df, table, col, spec):
    min_val = spec.get("min", 0)
    mask = df[col].notnull() & (df[col] < min_val)
    failed = df[mask]
    ok = failed.empty
    return _result(ok, "numeric_min", "column", table, column=col, failed_df=failed, details=f"min={min_val}")

def check_numeric_max(df, table, col, spec):
    max_val = spec.get("max", 10**12)
    mask = df[col].notnull() & (df[col] > max_val)
    failed = df[mask]
    ok = failed.empty
    return _result(ok, "numeric_max", "column", table, column=col, failed_df=failed, details=f"max={max_val}")

def check_timestamp_not_in_future(df, table, col, spec):
    now = pd.Timestamp.now(tz="UTC")
    ser = pd.to_datetime(df[col], errors="coerce", utc=True)
    mask = ser.notnull() & (ser > now)
    failed = df[mask]
    ok = failed.empty
    return _result(ok, "timestamp_not_in_future", "column", table, column=col, failed_df=failed)


def check_boolean(df, table, col, spec):
    mask = df[col].notnull() & ~df[col].isin([True, False])
    failed = df[mask]
    ok = failed.empty
    return _result(ok, "boolean", "column", table, column=col, failed_df=failed)

def check_conditional_required(df, table, col, spec):
    when_column = spec["when_column"]
    when_value = spec["when_value"]
    required_column = spec["required_column"]
    mask = (df[when_column] == when_value) & (df[required_column].isnull())
    failed = df[mask]
    ok = failed.empty
    return _result(ok, "conditional_required", "column", table, column=required_column, failed_df=failed,
                   details=f"when {when_column}={when_value}, {required_column} must NOT NULL")

CHECK_DISPATCH = {
    "row_count_min": ("table", check_row_count_min),
    "row_count_between": ("table", check_row_count_between),
    "not_null": ("column", check_not_null),
    "in_set": ("column", check_in_set),
    "regex": ("column", check_regex),
    "length_between": ("column", check_length_between),
    "numeric_min": ("column", check_numeric_min),
    "numeric_max": ("column", check_numeric_max),
    "timestamp_not_in_future": ("column", check_timestamp_not_in_future),
    "boolean": ("column", check_boolean),
    "conditional_required": ("column", check_conditional_required),
}

