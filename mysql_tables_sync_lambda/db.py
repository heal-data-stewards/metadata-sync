"""
Shared MySQL connection helpers for mysql_lambda.

Both helpers take an optional `database` override, for tables that read or
write a different database than DB_NAME (e.g. the tempspace/healstudies
split -- a table's own <TABLE>_DB_NAME env var, not DB_NAME, decides where
it lands; see tables/research_networks.py for the pattern). Omit it for the
original single-database behavior.
"""

import os

import mysql.connector
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
