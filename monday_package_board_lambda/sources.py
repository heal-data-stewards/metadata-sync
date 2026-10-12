"""
Input sources for the data package board pipeline.

Each monthly input is a file a person produces and places in a known
location (Excel for the CDE mapping metrics report, CSV for the platform
determination sheet). Reads go through a small source interface so that the
storage can change (S3 now, LakeFS later) without touching the pipeline.

The CDE mapping metrics report keeps its own dated name each month (e.g.
cde_mapping_metrics_report_2026-09-29_v2.xlsx) rather than a fixed one, so
find_latest() picks the most recently modified file matching a glob pattern
instead of requiring the exact name as an env var every month.
"""

import fnmatch
import io
from typing import Protocol
from urllib.parse import urlparse

import pandas as pd


class FileSource(Protocol):
    def read_excel(self, name: str, sheet_name: str | int = 0) -> pd.DataFrame: ...
    def read_csv(self, name: str, **kwargs) -> pd.DataFrame: ...
    def find_latest(self, pattern: str) -> str: ...


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

    def find_latest(self, pattern: str) -> str:
        """Most recently modified key under the prefix whose basename matches `pattern`."""
        paginator = self.client.get_paginator("list_objects_v2")
        candidates = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{self.prefix}/" if self.prefix else ""):
            for obj in page.get("Contents", []):
                name = obj["Key"].rsplit("/", 1)[-1]
                if fnmatch.fnmatch(name, pattern):
                    candidates.append((obj["LastModified"], name))
        if not candidates:
            raise FileNotFoundError(f"No object matching {pattern!r} under s3://{self.bucket}/{self.prefix}")
        return max(candidates)[1]

    def write_excel(self, name: str, df: pd.DataFrame) -> None:
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="xlsxwriter")
        self.client.put_object(Bucket=self.bucket, Key=self._key(name), Body=buf.getvalue())


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

    def find_latest(self, pattern: str) -> str:
        import os
        candidates = [
            (os.path.getmtime(self._path(n)), n)
            for n in os.listdir(self.directory)
            if fnmatch.fnmatch(n, pattern)
        ]
        if not candidates:
            raise FileNotFoundError(f"No file matching {pattern!r} in {self.directory}")
        return max(candidates)[1]

    def write_excel(self, name: str, df: pd.DataFrame) -> None:
        df.to_excel(self._path(name), index=False, engine="xlsxwriter")
