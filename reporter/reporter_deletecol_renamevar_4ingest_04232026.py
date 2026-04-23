# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 13:35:45 2026

@author: mariad
"""

#!/usr/bin/env python3
import argparse
import os
import sys
import pandas as pd

# Default paths (use the exact paths you provided)
DEFAULT_INPUT = r"C:\Users\mariad\OneDrive - Research Triangle Institute\Documents\HEAL Relational Database\MySQL updates\update_04222026_reporter\reporter_mysql_in_04222026.csv"
DEFAULT_MAPPING = r"C:\Users\mariad\OneDrive - Research Triangle Institute\Documents\HEAL Relational Database\MySQL updates\update_04222026_reporter\reporter_mysql_var_04232026.csv"

def load_mapping(mapping_path):
    """Try several mapping formats and return dict mapping reporter_in -> mysql_name."""
    try:
        dfm = pd.read_csv(mapping_path, dtype=str, keep_default_na=False)
        cols_lower = [c.lower() for c in dfm.columns]
        if 'reporter_in' in cols_lower and 'mysql_in' in cols_lower:
            col_map = {c.lower(): c for c in dfm.columns}
            left_col = col_map['reporter_in']
            right_col = col_map['mysql_in']
            return dict(zip(dfm[left_col].str.strip(), dfm[right_col].str.strip()))
        if dfm.shape[1] >= 2:
            left = dfm.iloc[:,0].astype(str).str.strip()
            right = dfm.iloc[:,1].astype(str).str.strip()
            return dict(zip(left, right))
    except Exception:
        pass

    # Fallback: parse lines like "reporter_in: colname" followed by "MySQL_in: mysqlname"
    mapping = {}
    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        i = 0
        while i < len(lines) - 1:
            a = lines[i]
            b = lines[i+1]
            if a.lower().startswith('reporter_in') and b.lower().startswith('mysql_in'):
                key = a.split(':',1)[1].strip()
                val = b.split(':',1)[1].strip()
                mapping[key] = val
                i += 2
                continue
            i += 1
    except Exception:
        pass

    return mapping

def compute_output_path_from_input(input_path):
    """Return output path in same folder as input, with first occurrence of 'in_' removed from basename.
       If no 'in_' found, append '_out' before extension to avoid overwriting input."""
    folder = os.path.dirname(os.path.abspath(input_path))
    base = os.path.basename(input_path)
    name, ext = os.path.splitext(base)
    if "in_" in name:
        new_name = name.replace("in_", "", 1)
    else:
        new_name = f"{name}_out"
    output_basename = new_name + ext
    output_path = os.path.join(folder, output_basename)
    # Safety: do not overwrite input file
    if os.path.abspath(output_path) == os.path.abspath(input_path):
        # append _out
        output_basename = f"{name}_out{ext}"
        output_path = os.path.join(folder, output_basename)
    return output_path

def main():
    p = argparse.ArgumentParser(description="Auto-save filtered & renamed reporter CSV (auto-confirm; remove 'in_' from input filename).")
    p.add_argument('-i', '--input', default=DEFAULT_INPUT, help='Input CSV path (default: provided input)')
    p.add_argument('-m', '--mapping', default=DEFAULT_MAPPING, help='Mapping CSV path (default: provided mapping)')
    p.add_argument('--preview-rows', type=int, default=5, help='Number of rows to show in preview (default 5)')
    p.add_argument('--case-insensitive', action='store_true', help='Match mapping keys to input columns case-insensitively')
    p.add_argument('--dry-run', action='store_true', help="Don't write output file; just show preview and planned output path")
    args = p.parse_args()

    input_path = args.input
    mapping_path = args.mapping

    if not os.path.exists(input_path):
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        sys.exit(2)
    if not os.path.exists(mapping_path):
        print(f"ERROR: mapping file not found: {mapping_path}", file=sys.stderr)
        sys.exit(3)

    mapping = load_mapping(mapping_path)
    if not mapping:
        print(f"ERROR: could not read mappings from: {mapping_path}", file=sys.stderr)
        sys.exit(4)

    try:
        df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    except Exception as e:
        print(f"ERROR: could not read input CSV {input_path}: {e}", file=sys.stderr)
        sys.exit(5)

    # Normalize mapping keys to actual input column names if case-insensitive requested
    if args.case_insensitive:
        col_lower_to_orig = {c.lower(): c for c in df.columns}
        mapping_ci = {}
        for k, v in mapping.items():
            orig = col_lower_to_orig.get(k.lower())
            if orig:
                mapping_ci[orig] = v
        mapping = mapping_ci
    else:
        # Only keep mapping entries that exactly match input columns
        mapping = {k: v for k, v in mapping.items() if k in df.columns}

    cols_to_keep = [c for c in df.columns if c in mapping]
    dropped = [c for c in df.columns if c not in mapping]

    if not cols_to_keep:
        print("WARNING: No columns in the input matched mapping keys. Nothing to write.", file=sys.stderr)
        if dropped:
            print("Input columns:", ", ".join(dropped))
        sys.exit(0)

    df_out = df[cols_to_keep].rename(columns=mapping)

    # Preview
    n = args.preview_rows
    print("\n=== Preview (first {} rows) of filtered & renamed data ===\n".format(n))
    print("Planned output columns ({}): {}".format(len(df_out.columns), ", ".join(df_out.columns)))
    print("\nFirst rows:\n")
    print(df_out.head(n).to_string(index=False))
    print("\nRows in input: {}, rows in preview: {}".format(len(df), min(n, len(df))))
    if dropped:
        print("\nDropped columns ({}): {}".format(len(dropped), ", ".join(dropped)))

    output_path = compute_output_path_from_input(input_path)
    print(f"\nPlanned output file (auto): {output_path}")

    if args.dry_run:
        print("Dry run: no file written.")
        return

    # Auto-save (no confirmation)
    try:
        df_out.to_csv(output_path, index=False)
        print(f"Wrote {len(df_out)} rows and {len(df_out.columns)} columns to {output_path}")
    except Exception as e:
        print(f"ERROR writing output: {e}", file=sys.stderr)
        sys.exit(6)

if __name__ == "__main__":
    main()