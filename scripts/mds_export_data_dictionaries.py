#!/usr/bin/env python3
"""
Download all data dictionaries from the HEAL MDS and export them as CSV files.

For each data dictionary:
  - A CSV is written to <output_dir>/<dd_guid>.csv  (one row per field)

Additionally:
  - An index CSV lists every dd with its title, field count, and associated studies.
  - With --dump-json, raw JSON is written to <output_dir>/json/<dd_guid>.json.
  - With --consistency-report, an Excel workbook is written with four sheets:
      SchemaSummary   — one row per dd: schema version, structure type, field count
      FieldKeyCoverage — one row per field-level key: how many dds use it, % coverage
      TypeValues       — distribution of values in the "type" field across all fields
      Inconsistencies  — dds that use deprecated/variant key names or mixed structures

Usage:
    python mds_export_data_dictionaries.py [OPTIONS]

Options:
    --mds-endpoint        MDS metadata base URL        (default: https://healdata.org/mds/metadata)
    --output-dir          Directory for per-dd CSVs    (default: ./mds_data_dictionaries)
    --index-file          Path for the index CSV       (default: <output_dir>/index.csv)
    --dump-json           Also write raw JSON per dd   (flag, default: off)
    --consistency-report  Path for consistency Excel   (default: <output_dir>/consistency_report.xlsx)
    --no-consistency-report  Skip the consistency report
"""

import csv
import json
import sys
import click
import requests
import pandas as pd
from collections import Counter, defaultdict
from pathlib import Path

DEFAULT_MDS_ENDPOINT = "https://healdata.org/mds/metadata"
DEFAULT_OUTPUT_DIR = "mds_data_dictionaries"

DATA_DICT_GUID_TYPE = "data_dictionary"
STUDY_GUID_TYPES = {
    "discovery_metadata",
    "unregistered_discovery_metadata",
    "discovery_metadata_archive",
}

FIELD_COLUMNS_PRIORITY = [
    "name", "title", "description", "type", "section", "module", "format",
    "constraints", "enumLabels", "encodings", "missingValues", "trueValues",
    "falseValues", "source", "notes",
]

# Keys that are deprecated/renamed variants of canonical names.
# If a dd uses the left key, it's flagged in Inconsistencies.
DEPRECATED_KEYS = {
    "module":    "section",
    "encodings": "enumLabels",
}


# ---------------------------------------------------------------------------
# MDS fetch
# ---------------------------------------------------------------------------

def fetch_mds(endpoint: str, chunk_size: int = 1000) -> dict:
    """Page through the MDS and return a single dict keyed by GUID."""
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
        print(f"  fetched {len(result)} records …")
    return result


# ---------------------------------------------------------------------------
# Parse data dictionary content
# ---------------------------------------------------------------------------

def payload_structure(dd_record: dict) -> str:
    """
    Return a short label describing how the data_dictionary payload is structured:
      'dict.fields'          — dict with a "fields" key  (current canonical form)
      'dict.data_dictionary' — dict with a "data_dictionary" key  (legacy)
      'list'                 — bare list of field dicts  (legacy)
      'empty'                — present but empty
      'missing'              — key absent entirely
    """
    payload = dd_record.get("data_dictionary")
    if payload is None:
        return "missing"
    if isinstance(payload, list):
        return "empty" if not payload else "list"
    if isinstance(payload, dict):
        if payload.get("fields") is not None:
            return "dict.fields"
        if payload.get("data_dictionary") is not None:
            return "dict.data_dictionary"
        return "empty"
    return "unknown"


def extract_fields(dd_record: dict) -> list[dict]:
    """
    Return a list of field dicts from a data_dictionary MDS record.
    Handles all three known payload structures.
    """
    payload = dd_record.get("data_dictionary")
    if payload is None:
        return []
    if isinstance(payload, list):
        return [f for f in payload if isinstance(f, dict)]
    if isinstance(payload, dict):
        fields = payload.get("fields") or payload.get("data_dictionary") or []
        return [f for f in fields if isinstance(f, dict)]
    return []


def fields_to_rows(fields: list[dict]) -> list[dict]:
    """Normalise field dicts — complex values serialised as JSON strings."""
    all_keys: list[str] = []
    seen: set[str] = set()
    for f in fields:
        for k in f:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    ordered = [k for k in FIELD_COLUMNS_PRIORITY if k in seen]
    ordered += [k for k in all_keys if k not in set(ordered)]

    rows = []
    for f in fields:
        row = {}
        for k in ordered:
            v = f.get(k, "")
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False)
            elif v is None:
                v = ""
            row[k] = v
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Study ↔ dd mapping
# ---------------------------------------------------------------------------

def build_study_dd_map(metadata: dict) -> dict[str, list[dict]]:
    """Return {dd_guid: [{study_guid, hdp_id, label}, …]} for every study."""
    dd_to_studies: dict[str, list[dict]] = {}
    for study_guid, record in metadata.items():
        if record.get("_guid_type") not in STUDY_GUID_TYPES:
            continue
        gen3 = record.get("gen3_discovery", {})
        hdp_id = gen3.get("_hdp_uid", "")
        data_dicts = record.get("variable_level_metadata", {}).get("data_dictionaries", {})
        if not isinstance(data_dicts, dict):
            continue
        for label, dd_guid in data_dicts.items():
            if dd_guid:
                dd_to_studies.setdefault(dd_guid, []).append(
                    {"study_guid": study_guid, "hdp_id": hdp_id, "label": label}
                )
    return dd_to_studies


# ---------------------------------------------------------------------------
# Consistency analysis
# ---------------------------------------------------------------------------

def build_consistency_report(dd_records: dict) -> dict[str, pd.DataFrame]:
    """
    Analyse all dd records and return a dict of DataFrames keyed by sheet name.

    Sheets:
      SchemaSummary    — one row per dd
      FieldKeyCoverage — one row per field-level key
      TypeValues       — distribution of "type" field values
      Inconsistencies  — dds using deprecated keys or mixed structures
    """

    schema_rows = []
    # key → set of dd_guids that use it
    key_to_dds: defaultdict[str, set] = defaultdict(set)
    # key → total occurrence count across all fields
    key_occurrences: Counter = Counter()
    type_counter: Counter = Counter()
    inconsistency_rows = []

    for dd_guid, dd_rec in sorted(dd_records.items()):
        title = dd_rec.get("title", "")
        structure = payload_structure(dd_rec)

        payload = dd_rec.get("data_dictionary", {})
        schema_version = ""
        if isinstance(payload, dict):
            schema_version = payload.get("schemaVersion", "")

        fields = extract_fields(dd_rec)
        field_count = len(fields)

        # Collect all keys used in this dd's fields.
        dd_keys: set[str] = set()
        for f in fields:
            for k, v in f.items():
                dd_keys.add(k)
                key_occurrences[k] += 1
                if k == "type" and isinstance(v, str) and v:
                    type_counter[v] += 1

        for k in dd_keys:
            key_to_dds[k].add(dd_guid)

        schema_rows.append({
            "dd_guid": dd_guid,
            "dd_title": title,
            "schema_version": schema_version,
            "structure_type": structure,
            "field_count": field_count,
            "unique_field_keys": len(dd_keys),
            "field_keys": "||".join(sorted(dd_keys)),
        })

        # Flag deprecated key usage.
        for dep_key, canonical in DEPRECATED_KEYS.items():
            if dep_key in dd_keys:
                inconsistency_rows.append({
                    "dd_guid": dd_guid,
                    "dd_title": title,
                    "issue": f"uses '{dep_key}' instead of canonical '{canonical}'",
                    "deprecated_key": dep_key,
                    "canonical_key": canonical,
                    "also_has_canonical": canonical in dd_keys,
                })

        # Flag non-canonical structure.
        if structure != "dict.fields":
            inconsistency_rows.append({
                "dd_guid": dd_guid,
                "dd_title": title,
                "issue": f"non-canonical payload structure: '{structure}'",
                "deprecated_key": "",
                "canonical_key": "",
                "also_has_canonical": False,
            })

    total_dds = len(dd_records)

    # --- Sheet 1: SchemaSummary ---
    schema_df = pd.DataFrame(schema_rows)

    # --- Sheet 2: FieldKeyCoverage ---
    coverage_rows = []
    for key, dd_set in sorted(key_to_dds.items()):
        dd_count = len(dd_set)
        coverage_rows.append({
            "field_key": key,
            "dd_count": dd_count,
            "dd_pct": round(100 * dd_count / total_dds, 1) if total_dds else 0,
            "total_occurrences": key_occurrences[key],
            "in_all_dds": dd_count == total_dds,
            "is_deprecated": key in DEPRECATED_KEYS,
            "canonical_name": DEPRECATED_KEYS.get(key, ""),
        })
    coverage_df = pd.DataFrame(coverage_rows).sort_values(
        ["dd_count", "field_key"], ascending=[False, True]
    ).reset_index(drop=True)

    # --- Sheet 3: TypeValues ---
    type_df = pd.DataFrame(
        [{"type_value": t, "occurrence_count": c} for t, c in type_counter.most_common()],
    )

    # --- Sheet 4: Inconsistencies ---
    inconsistency_df = pd.DataFrame(inconsistency_rows) if inconsistency_rows else pd.DataFrame(
        columns=["dd_guid", "dd_title", "issue", "deprecated_key", "canonical_key", "also_has_canonical"]
    )

    return {
        "SchemaSummary": schema_df,
        "FieldKeyCoverage": coverage_df,
        "TypeValues": type_df,
        "Inconsistencies": inconsistency_df,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

@click.command()
@click.option("--mds-endpoint", default=DEFAULT_MDS_ENDPOINT, show_default=True,
              help="MDS metadata base URL.")
@click.option("--output-dir", default=DEFAULT_OUTPUT_DIR, show_default=True,
              help="Directory where per-dd CSV files are written.")
@click.option("--index-file", default=None,
              help="Path for the index CSV (default: <output_dir>/index.csv).")
@click.option("--dump-json", is_flag=True, default=False,
              help="Write raw JSON for each data dictionary to <output_dir>/json/.")
@click.option("--consistency-report", "consistency_report_path", default=None,
              help="Path for the consistency Excel report "
                   "(default: <output_dir>/consistency_report.xlsx). "
                   "Pass --no-consistency-report to skip.")
@click.option("--no-consistency-report", "skip_consistency", is_flag=True, default=False,
              help="Skip writing the consistency report.")
@click.option("--chunk-size", default=1000, show_default=True,
              help="Number of records per MDS request.")
def main(
    mds_endpoint: str,
    output_dir: str,
    index_file: str,
    dump_json: bool,
    consistency_report_path: str,
    skip_consistency: bool,
    chunk_size: int,
) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    index_path = Path(index_file) if index_file else out_dir / "index.csv"

    json_dir = out_dir / "json"
    if dump_json:
        json_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching metadata from {mds_endpoint} …")
    metadata = fetch_mds(mds_endpoint, chunk_size)
    print(f"Fetched {len(metadata)} total records.")

    dd_records = {
        guid: rec for guid, rec in metadata.items()
        if rec.get("_guid_type") == DATA_DICT_GUID_TYPE
    }
    print(f"Found {len(dd_records)} data dictionaries.")

    print("Building study ↔ data dictionary map …")
    dd_to_studies = build_study_dd_map(metadata)

    index_rows = []
    written = 0
    skipped = 0

    for dd_guid, dd_rec in sorted(dd_records.items()):
        # Optionally dump raw JSON.
        if dump_json:
            with open(json_dir / f"{dd_guid}.json", "w", encoding="utf-8") as jf:
                json.dump(dd_rec, jf, indent=2, ensure_ascii=False)

        fields = extract_fields(dd_rec)
        studies = dd_to_studies.get(dd_guid, [])
        common = {
            "dd_guid": dd_guid,
            "dd_title": dd_rec.get("title", ""),
            "studies_count": len(studies),
            "study_hdp_ids": "||".join(s["hdp_id"] for s in studies if s["hdp_id"]),
            "study_guids": "||".join(s["study_guid"] for s in studies),
            "study_labels": "||".join(s["label"] for s in studies),
        }

        if not fields:
            print(f"  [skip] {dd_guid} — no fields found", file=sys.stderr)
            skipped += 1
            index_rows.append({**common, "csv_file": "", "field_count": 0, "error": "no fields"})
            continue

        rows = fields_to_rows(fields)
        csv_filename = f"{dd_guid}.csv"
        all_keys = list(rows[0].keys()) if rows else []
        with open(out_dir / csv_filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        index_rows.append({**common, "csv_file": csv_filename, "field_count": len(rows), "error": ""})
        written += 1

    pd.DataFrame(index_rows).to_csv(index_path, index=False)

    # Consistency report.
    if not skip_consistency:
        print("Running consistency analysis …")
        sheets = build_consistency_report(dd_records)

        report_path = Path(consistency_report_path) if consistency_report_path \
            else out_dir / "consistency_report.xlsx"

        writer = pd.ExcelWriter(report_path, engine="openpyxl")
        try:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            writer.close()
        except Exception as exc:
            try:
                writer.close()
            except Exception:
                pass
            print(f"Warning: could not write consistency report: {exc}", file=sys.stderr)
        else:
            print(f"  Consistency report      : {report_path}")
            incons = sheets["Inconsistencies"]
            coverage = sheets["FieldKeyCoverage"]
            total = len(dd_records)
            rare = coverage[coverage["dd_pct"] < 50]
            print(
                f"  Schema versions seen    : "
                + ", ".join(
                    f"{v!r} ({n})"
                    for v, n in sheets["SchemaSummary"]["schema_version"]
                    .value_counts().items()
                )
            )
            print(f"  Field keys seen         : {len(coverage)} unique keys")
            print(f"  Keys in <50% of dds     : {len(rare)}")
            print(f"  Inconsistencies flagged : {len(incons)}")

    print(
        f"\nDone.\n"
        f"  Data dictionaries written : {written}\n"
        f"  Skipped (no fields)       : {skipped}\n"
        f"  Index CSV                 : {index_path}\n"
        f"  Per-dd CSVs               : {out_dir}/\n"
        + (f"  Raw JSON files            : {json_dir}/\n" if dump_json else "")
    )


if __name__ == "__main__":
    main()
