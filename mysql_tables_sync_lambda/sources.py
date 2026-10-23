"""
Manual input sources for mysql_tables_sync_lambda.

Some tables need a hand-maintained reference file (e.g. research_networks'
Google-Drive-sourced lookup workbook, or HEAL_02's correct_foanoa_values.csv
correction file) that a person uploads to S3 rather than something synced
automatically. Reads go through a small source interface so a table module
doesn't care whether it's running against S3 (Lambda) or a local directory
(tests, local runs).
"""

import io
from typing import Protocol
from urllib.parse import urlparse

import pandas as pd


class FileSource(Protocol):
    def read_excel(self, name: str, sheet_name: str | int = 0) -> pd.DataFrame: ...
    def read_csv(self, name: str, **kwargs) -> pd.DataFrame: ...
    def write_csv(self, name: str, df: pd.DataFrame) -> None: ...
    def write_bytes(self, name: str, data: bytes) -> None: ...


class S3ExcelSource:
    """Reads Excel/CSV files from s3://<bucket>/<prefix>/<name>."""

    def __init__(self, s3_uri: str, s3_client=None):
        parsed = urlparse(s3_uri)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"Expected s3://bucket/prefix, got {s3_uri!r}")
        self.bucket = parsed.netloc
        self.prefix = parsed.path.strip("/")
        if s3_client is None:
            import boto3
            s3_client = boto3.client("s3")
        self.client = s3_client

    def _key(self, name: str) -> str:
        return f"{self.prefix}/{name}" if self.prefix else name

    def _get(self, name: str) -> bytes:
        obj = self.client.get_object(Bucket=self.bucket, Key=self._key(name))
        return obj["Body"].read()

    def read_excel(self, name: str, sheet_name: str | int = 0) -> pd.DataFrame:
        return pd.read_excel(io.BytesIO(self._get(name)), sheet_name=sheet_name)

    def read_csv(self, name: str, **kwargs) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(self._get(name)), **kwargs)

    def write_csv(self, name: str, df: pd.DataFrame) -> None:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        self.client.put_object(Bucket=self.bucket, Key=self._key(name), Body=buf.getvalue().encode("utf-8"))

    def write_bytes(self, name: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._key(name), Body=data)


class LocalExcelSource:
    """Reads Excel/CSV files from a local directory. Used for local runs and tests."""

    def __init__(self, directory: str):
        self.directory = directory

    def _path(self, name: str) -> str:
        import os
        return os.path.join(self.directory, name)

    def read_excel(self, name: str, sheet_name: str | int = 0) -> pd.DataFrame:
        return pd.read_excel(self._path(name), sheet_name=sheet_name)

    def read_csv(self, name: str, **kwargs) -> pd.DataFrame:
        return pd.read_csv(self._path(name), **kwargs)

    def write_csv(self, name: str, df: pd.DataFrame) -> None:
        import os
        path = self._path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)

    def write_bytes(self, name: str, data: bytes) -> None:
        import os
        path = self._path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
