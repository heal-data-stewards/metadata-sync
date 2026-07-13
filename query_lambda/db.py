"""MySQL connection helper for query_lambda."""

import os

import mysql.connector


def connect_mysql():
    """Return a mysql.connector connection from env vars."""
    return mysql.connector.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
    )
