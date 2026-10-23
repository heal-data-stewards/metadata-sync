"""
Shared MySQL connection helpers for mysql_lambda.

connect_mysql/create_alchemy_engine both take an optional `database`
override, for tables that read or write a different database than DB_NAME
(e.g. the tempspace/healstudies split -- a table's own <TABLE>_DB_NAME env
var, not DB_NAME, decides where it lands; see tables/research_networks.py
for the pattern). Omit it for the original single-database behavior.

clean_mysql_value/format_mysql_write_value fix two related bugs discovered
by diffing real Lambda output against the original Stata-era CSVs:
mysql.connector returns native Python types for non-VARCHAR MySQL columns
-- a SET column comes back as a Python `set` (str()'d naively as
"{'x'}", or "set()" when empty) and a numeric column comes back as
float/Decimal (str()'d naively as "247500.0" for a whole number) -- both
break equality/string comparisons against values that were always plain
strings elsewhere in this pipeline. Every table module's own _read_table/
_write_table should route through these instead of a bare str()/isinstance
check, so a fix here reaches all of them -- the SET-flattening fix had
previously been added only to study_summary.py's own copy and silently
never made it into the other four modules' copies, which is exactly the
failure mode importing a shared helper avoids.
"""

import datetime as _dt
import os
from decimal import Decimal

import mysql.connector
import pandas as pd
from sqlalchemy import create_engine


def connect_mysql(database: str | None = None):
    """Return a mysql.connector connection from env vars."""
    return mysql.connector.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        database=database or os.getenv("DB_NAME"),
    )


def create_alchemy_engine(database: str | None = None):
    """Return a SQLAlchemy engine from env vars (used by reporter table)."""
    u = os.getenv("DB_USER")
    p = os.getenv("DB_PASSWORD")
    h = os.getenv("DB_HOST")
    db = database or os.getenv("DB_NAME")
    return create_engine(f"mysql+pymysql://{u}:{p}@{h}/{db}")


def clean_mysql_value(v):
    """Normalize one raw value from a mysql.connector cursor row into
    something safe for this pipeline's string-based processing. Apply to
    every column right after SELECT * -- see module docstring for why."""
    if v is None:
        return None
    if isinstance(v, set):
        return ",".join(sorted(v)) if v else None
    if isinstance(v, (float, Decimal)):
        try:
            if v != v:  # NaN
                return None
        except TypeError:
            pass
        fv = float(v)
        return str(int(fv)) if fv == int(fv) else str(v)
    if isinstance(v, _dt.datetime):
        return v.strftime("%Y-%m-%d") if v.time() == _dt.time(0, 0, 0) else v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, _dt.date):
        return v.strftime("%Y-%m-%d")
    return v


def format_mysql_write_value(v):
    """Normalize one pandas cell into a string (or None) for a TEXT-column
    INSERT. Same date-formatting rule as clean_mysql_value, so a
    pd.Timestamp column built via pd.to_datetime() round-trips as a clean
    date instead of picking up a spurious 00:00:00 time -- and the same
    whole-number-float rule, so a column that upcast to float64 along the
    way (e.g. via pd.to_numeric(), or a merge introducing NaN into what
    was an int column) round-trips as "2020", not "2020.0"."""
    if pd.isna(v):
        return None
    if isinstance(v, (pd.Timestamp, _dt.datetime)):
        return v.strftime("%Y-%m-%d") if v.time() == _dt.time(0, 0, 0) else v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, _dt.date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, (float, Decimal)):
        fv = float(v)
        return str(int(fv)) if fv == int(fv) else str(v)
    return str(v)
