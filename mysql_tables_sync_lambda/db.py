"""Shared MySQL connection helpers for mysql_lambda."""

import os

import mysql.connector
from sqlalchemy import create_engine


def connect_mysql():
    """Return a mysql.connector connection from env vars."""
    return mysql.connector.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
    )


def create_alchemy_engine():
    """Return a SQLAlchemy engine from env vars (used by reporter table)."""
    u = os.getenv("DB_USER")
    p = os.getenv("DB_PASSWORD")
    h = os.getenv("DB_HOST")
    db = os.getenv("DB_NAME")
    return create_engine(f"mysql+pymysql://{u}:{p}@{h}/{db}")
