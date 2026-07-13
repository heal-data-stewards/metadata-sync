"""
HEAL Monday Board Update (v2)

Reads from the monday_studies_mysql table (populated by lambda_function.py),
formats the data for the HEAL Studies Monday board, and exports xlsx batches
ready for upload.

Usage:
    python monday_board_update.py
    python monday_board_update.py --output-dir /tmp/monday --debug
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import click
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from lambda_function import RENAME_DICT, _connect, _read_table, _mysql_colname

load_dotenv()

# ---------------------------------------------------------------------------
# Column mapping: monday_studies_mysql snake_case name → Monday display name
# ---------------------------------------------------------------------------

# The Lambda writes Monday display names through _mysql_colname(), so we need
# the reverse to get back to Monday names before calling prepare_for_monday().
MYSQL_TO_MONDAY = {_mysql_colname(v): v for v in RENAME_DICT.values()}

# Columns that are stored as-is (already snake_case, no rename needed)
_PASSTHROUGH = {
    'study_most_recent_appl', 'study_hdp_id', 'study_hdp_id_appl',
    'key', 'hdp_id',
}


# ---------------------------------------------------------------------------
# Read from monday_studies_mysql
# ---------------------------------------------------------------------------

def read_monday_studies(conn, table: str = 'monday_studies_mysql') -> pd.DataFrame:
    """Read monday_studies_mysql and restore Monday Board display column names."""
    df = _read_table(conn, table)
    rename = {k: v for k, v in MYSQL_TO_MONDAY.items() if k in df.columns}
    return df.rename(columns=rename)


# ---------------------------------------------------------------------------
# Prepare for Monday Board  (ported from scripts/monday_board_update.py)
# ---------------------------------------------------------------------------

def _parse_date(val):
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(str(val), fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def prepare_for_monday(all_data: pd.DataFrame) -> pd.DataFrame:
    data = all_data.copy()

    # Study type classification
    data['study_type'] = [
        'CTN' if str(m).startswith('CTN') else ('APPLIDONLY' if pd.isna(k) else 'HDP')
        for m, k in data[['Project #', 'study_hdp_id_appl']].values
    ]

    # Location
    data['City']     = data[['City']].fillna('-')
    data['State']    = data[['State']].fillna('-')
    data['Location'] = [f"{c},{s}" for c, s in data[['City', 'State']].values]

    # Dates
    data['Project Start']    = data['Project Start'].apply(_parse_date).fillna('-')
    data['Project End']      = data['Project End'].apply(_parse_date).fillna('-')
    data['Platform Reg Time'] = pd.to_datetime(
        data['Platform Reg Time'], utc=True, errors='coerce'
    ).dt.date
    data['Platform Reg Time'] = data['Platform Reg Time'].fillna('-')

    # Flag columns
    data['Archived']         = [a if a == 'archived' else 'n' for a in data['Archived']]
    data['HEAL-Related']     = [
        'Y' if (p != 'CTN' and pd.isna(a)) else 'N'
        for p, a in data[['study_type', 'HEAL-Related']].values
    ]
    data['SBIR/STTR']        = ['Y' if 'SBIR/STTR' == t else 'N' for t in data['SBIR/STTR']]
    data['Checklist Exempt'] = ['Y' if str(t) == '1' else 'N' for t in data['Checklist Exempt']]
    data['Do not Engage']    = ['Y' if str(t) == '1' else 'N' for t in data['Do not Engage']]

    # Rename internal columns to Monday Board names
    data.rename(columns={
        'study_most_recent_appl': 'Most Recent Appl_ID',
        'study_hdp_id_appl':      'HDP appl_ID',
    }, inplace=True)
    data.drop(
        columns=['study_hdp_id', 'hdp_id', 'hdp_id_x', 'hdp_id_y'],
        errors='ignore',
        inplace=True,
    )

    # Fill remaining string NaNs with '-'
    handled = {
        'study_type', 'City', 'State', 'Location', 'Project Start', 'Project End',
        'Platform Reg Time', 'Archived', 'HEAL-Related', 'SBIR/STTR',
        'Checklist Exempt', 'Do not Engage',
    }
    for col in data.columns:
        if col not in handled and data[col].dtype == object:
            data[col] = ['-' if (v is np.nan or v == '') else v for v in data[col]]

    return data


# ---------------------------------------------------------------------------
# Export to xlsx
# ---------------------------------------------------------------------------

def export_finaldata(output_dir: Path, final_dataset: pd.DataFrame):
    output_dir.mkdir(parents=True, exist_ok=True)

    final_dataset.reset_index(drop=True, inplace=True)
    final_dataset.index.name = 'index'

    key_counts = final_dataset.groupby('key').size()
    t = key_counts.describe()
    logging.info(f"Records in final dataset: {len(final_dataset)}")
    logging.info(f"One row per key? {bool(t['min'] == 1 and t['max'] == 1)}")

    # Full export
    outfile = output_dir / 'MondayBoard_Update.xlsx'
    final_dataset.to_excel(outfile, engine='xlsxwriter', index=True)
    logging.info(f"Exported full file → {outfile}")

    # Batched exports (Monday has an import row limit)
    batch_size = 1400
    n_batches  = (len(final_dataset) - 1) // batch_size + 1
    for i in range(n_batches):
        start = i * batch_size
        end   = min(start + batch_size, len(final_dataset))
        batch_file = output_dir / f"MondayBoard_Update_batch_{i+1}_records_{start+1}_to_{end}.xlsx"
        final_dataset.iloc[start:end].to_excel(batch_file, engine='xlsxwriter', index=True)
        logging.info(f"Batch {i+1} ({end-start} records) → {batch_file}")

    logging.info("Export complete.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@click.command()
@click.option(
    '--output-dir', default='/tmp/monday',
    help='Directory where xlsx files will be written.',
)
@click.option(
    '--debug', is_flag=True, default=False,
    help='Enable debug logging.',
)
def create_monday_update_file(output_dir: str, debug: bool):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        stream=sys.stdout,
    )

    conn = _connect()

    logging.info('Reading monday_studies_mysql')
    df = read_monday_studies(conn)
    logging.info(f'  {len(df)} rows loaded')

    logging.info('Preparing for Monday Board')
    final = prepare_for_monday(df)

    logging.info(f'Exporting to {output_dir}')
    export_finaldata(Path(output_dir), final)

    conn.close()


if __name__ == '__main__':
    create_monday_update_file()
