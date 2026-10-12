"""
MySQL connection helper for monday_studies_board_lambda.

Connects directly to DB_HOST (the Lambda runs inside the VPC, same as
mysql_tables_sync_lambda) -- no SSH tunnel. Vendored from
mysql_tables_sync_lambda/db.py rather than imported across Lambda
directories, since each Lambda's deploy zip is flat and self-contained.
"""

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
