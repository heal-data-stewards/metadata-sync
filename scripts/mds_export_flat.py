#!/usr/bin/env python3
"""
Query the HEAL MDS and export a flattened metadata snapshot to Excel.

Two sheets are produced:
  StudyData  — one row per study, columns = dotted leaf-node field paths
               (e.g. gen3_discovery.study_metadata.investigators_name).
               List values whose items are all scalars are joined with "||".
               Lists that contain nested objects are indexed as field[0], field[1], …

  FieldNames — one row per unique field path, broken into level_1 / level_2 / …
               columns so the nesting depth is visible at a glance.

Usage:
    python mds_export_flat.py [OPTIONS]

Options:
    --mds-endpoint  MDS metadata base URL  (default: https://healdata.org/mds/metadata)
    --output        Output Excel file path (default: mds_metadata.xlsx)
    --chunk-size    Records per MDS request (default: 1000)
"""

import json
import re
import sys
import click
import requests
import pandas as pd
from pathlib import Path

# XML 1.0 prohibits control characters except tab (0x09), LF (0x0A), CR (0x0D).
# openpyxl raises IllegalCharacterError when these appear in cell values.
_ILLEGAL_XML_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _sanitize(val):
    """Strip illegal XML characters from string values."""
    if isinstance(val, str):
        return _ILLEGAL_XML_CHARS.sub("", val)
    return val


DEFAULT_MDS_ENDPOINT = "https://healdata.org/mds/metadata"
DEFAULT_OUTPUT = "mds_metadata.xlsx"
STUDY_GUID_TYPES = [
    "discovery_metadata",
    "unregistered_discovery_metadata",
    "discovery_metadata_archive",
]


# ---------------------------------------------------------------------------
# MDS fetch
# ---------------------------------------------------------------------------

def fetch_mds(endpoint: str, chunk_size: int) -> dict:
    """Page through the MDS and return a single dict keyed by study GUID."""
    result = {}
    for offset in range(0, 100_000, chunk_size):
        url = f"{endpoint}?data=True&limit={chunk_size}&offset={offset}"
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Warning: request failed at offset {offset}: {e}", file=sys.stderr)
            break
        chunk = resp.json()
        if not chunk:
            break
        result.update(chunk)
        print(f"  fetched {len(result)} studies so far …")
    return result


# ---------------------------------------------------------------------------
# JSON flattening
# ---------------------------------------------------------------------------

def flatten(obj, prefix: str = "", sep: str = ".") -> dict:
    """
    Recursively flatten a nested JSON value to {dotted_path: leaf_value}.

    - Dicts: recurse with key appended.
    - Lists of scalars: join with "||" and emit as a single leaf.
    - Lists containing any dicts/lists: serialize the whole list as a JSON
      string and emit as a single leaf.  Expanding with [0],[1],… indices
      causes column explosion (e.g. __manifest can have 4000+ entries).
    - Scalars: emit directly.
    """
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            child = f"{prefix}{sep}{k}" if prefix else k
            out.update(flatten(v, child, sep))
    elif isinstance(obj, list):
        if not obj:
            out[prefix] = ""
        elif all(not isinstance(v, (dict, list)) for v in obj):
            out[prefix] = "||".join("" if v is None else str(v) for v in obj)
        else:
            out[prefix] = json.dumps(obj, ensure_ascii=False)
    else:
        out[prefix] = obj
    return out


# ---------------------------------------------------------------------------
# Build sheets
# ---------------------------------------------------------------------------

def build_study_data(metadata: dict) -> pd.DataFrame:
    rows = []
    for guid, study in metadata.items():
        flat = flatten(study)
        flat["_guid"] = guid
        rows.append(flat)

    df = pd.DataFrame(rows)
    # Move _guid to the first column.
    cols = ["_guid"] + [c for c in df.columns if c != "_guid"]
    return df[cols]


def build_field_names(study_df: pd.DataFrame) -> pd.DataFrame:
    fields = sorted(c for c in study_df.columns if c != "_guid")
    if not fields:
        return pd.DataFrame(columns=["full_path"])

    max_depth = max(len(f.split(".")) for f in fields)
    rows = []
    for field in fields:
        parts = field.split(".")
        row: dict = {"full_path": field}
        for i, part in enumerate(parts):
            row[f"level_{i + 1}"] = part
        rows.append(row)

    level_cols = [f"level_{i + 1}" for i in range(max_depth)]
    df = pd.DataFrame(rows)
    ordered = ["full_path"] + [c for c in level_cols if c in df.columns]
    return df[ordered]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option(
    "--mds-endpoint",
    default=DEFAULT_MDS_ENDPOINT,
    show_default=True,
    help="MDS metadata base URL.",
)
@click.option(
    "--output",
    default=DEFAULT_OUTPUT,
    show_default=True,
    help="Output Excel (.xlsx) file path.",
)
@click.option(
    "--chunk-size",
    default=1000,
    show_default=True,
    help="Number of records to request per MDS call.",
)
@click.option(
    "--guid-types",
    default=",".join(STUDY_GUID_TYPES),
    show_default=True,
    help="Comma-separated list of _guid_type values to include. "
         "Pass 'all' to include every record type.",
)
def main(mds_endpoint: str, output: str, chunk_size: int, guid_types: str) -> None:
    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching metadata from {mds_endpoint} …")
    metadata = fetch_mds(mds_endpoint, chunk_size)
    print(f"Fetched {len(metadata)} total records.")

    if guid_types.strip().lower() != "all":
        keep = {t.strip() for t in guid_types.split(",")}
        metadata = {
            k: v for k, v in metadata.items()
            if v.get("_guid_type") in keep
        }
        print(f"Filtered to {len(metadata)} records with _guid_type in {keep}.")

    print("Flattening study data …")
    study_df = build_study_data(metadata)
    field_df = build_field_names(study_df)

    # Excel hard limit: 16,384 columns (including the _guid column).
    MAX_EXCEL_COLS = 16_384
    if len(study_df.columns) > MAX_EXCEL_COLS:
        print(
            f"Warning: {len(study_df.columns)} columns exceed Excel's limit of "
            f"{MAX_EXCEL_COLS}. StudyData sheet will be truncated to the first "
            f"{MAX_EXCEL_COLS} columns. All field paths are still in FieldNames."
        )
        study_df = study_df.iloc[:, :MAX_EXCEL_COLS]

    study_df = study_df.map(_sanitize)
    field_df = field_df.map(_sanitize)

    print(f"Writing {out_path} …")
    writer = pd.ExcelWriter(out_path, engine="openpyxl")
    try:
        study_df.to_excel(writer, sheet_name="StudyData", index=False)
        field_df.to_excel(writer, sheet_name="FieldNames", index=False)
        writer.close()
    except Exception as exc:
        # Ensure the writer is closed even on failure so the file isn't locked.
        try:
            writer.close()
        except Exception:
            pass
        raise RuntimeError(
            f"Failed to write Excel file: {exc}\n"
            f"StudyData had {len(study_df)} rows × {len(study_df.columns)} columns."
        ) from exc

    print(
        f"Done.  {len(study_df)} studies × {len(study_df.columns) - 1} fields"
        f"  →  {out_path}"
    )
    print(f"       {len(field_df)} unique field paths in FieldNames sheet.")


if __name__ == "__main__":
    main()
