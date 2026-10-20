"""
Monday Studies Board — pre-upload email backfill + comparison.

Run this by hand, right before uploading to Monday, after:
  1. monday_studies_board_lambda has written MondayStudiesBoard_<date>.xlsx to
     S3 and you've downloaded it locally, and
  2. you've exported the *current* Monday Studies board from Monday's UI
     (HEAL_Studies_*.xlsx) -- the live board can have human edits since the
     last sync, so this step always wants today's export, not an old one.

This intentionally runs AFTER the Lambda, not interleaved with it: the
Lambda's job is "turn study_summary into a Monday-shaped file," full stop.
Backfill and comparison both need the live board, which only exists at
upload time, so they live here instead -- and purely locally. Nothing here
is uploaded to S3; the output is a ready-to-upload file plus a few audit
CSVs for you to read, all in --output-dir.

Usage:
    python compare_monday_studies_board.py \\
        --new-board /path/to/MondayStudiesBoard_2026-10-12.xlsx \\
        --monday-export /path/to/dir/with/HEAL_Studies_export.xlsx \\
        --output-dir /path/to/output
"""

import logging
import sys
from pathlib import Path

import click
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 1400  # matches monday_studies_board_lambda

# Pseudo-rows Monday's own UI export includes that aren't real studies.
_SECTION_HEADER_ROWS = [
    "Studies under investigation",
    "Name",
    "Studies Not Added to Platform",
    "CTN Protocols",
    "Pending assessment results",
]


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

def read_new_board(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    logger.info("New board (%s): %d rows", path.name, len(df))
    return df


def import_monday_board(input_dir: Path, file_name: str = "HEAL_Studies_*.xlsx", rows_to_skip: int = 4) -> pd.DataFrame:
    board_files = list(Path(input_dir).glob(file_name)) if Path(input_dir).is_dir() else [Path(input_dir)]
    if not board_files:
        raise FileNotFoundError(f"No Monday board export found matching {input_dir}/{file_name}")
    df = pd.read_excel(board_files[0], skiprows=rows_to_skip)
    monday_board = df[~df["Name"].isin(_SECTION_HEADER_ROWS)]
    monday_board = monday_board[~pd.isna(monday_board["Name"])]
    logger.info("Live Monday export (%s): %d rows", board_files[0].name, len(monday_board))
    return monday_board


# ---------------------------------------------------------------------------
# Email backfill -- the one place the new board's content actually changes
# ---------------------------------------------------------------------------

def backfill_emails_from_monday(new_board: pd.DataFrame, monday_board: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    """Where the new board has no Contact Email ('-'), fill in from the live Monday export."""
    monday_emails = monday_board[["Most Recent Appl_ID", "Contact Email"]].drop_duplicates().copy()
    monday_emails["Contact Email"] = monday_emails["Contact Email"].replace("-", "").fillna("")
    monday_emails = (
        monday_emails.sort_values("Contact Email", ascending=False)
        .drop_duplicates(subset="Most Recent Appl_ID", keep="first")
    )

    merged = pd.merge(
        new_board, monday_emails, how="left",
        on="Most Recent Appl_ID", suffixes=("", "_monday"),
    )

    new_email = merged["Contact Email"].astype(str).str.strip()
    monday_email = merged["Contact Email_monday"].astype(str).str.strip()
    # A study absent from the live export (e.g. not yet on Monday at all) has a
    # genuinely-NaN Contact Email_monday after the merge -- str(NaN) == "nan",
    # which is non-empty, so the has-a-real-email check must also require notna()
    # or that NaN itself gets backfilled in as the "replacement" value.
    has_monday_email = merged["Contact Email_monday"].notna() & (monday_email.str.len() > 1)
    backfilled_mask = new_email.isin(["", "-", "nan", "None"]) & has_monday_email

    merged["Contact Email"] = merged["Contact Email"].where(~backfilled_mask, merged["Contact Email_monday"])
    merged.drop(columns=["Contact Email_monday"], errors="ignore", inplace=True)

    logger.info("Email backfill: %d row(s) filled in from the live Monday board", int(backfilled_mask.sum()))
    merged.loc[backfilled_mask, ["key", "Most Recent Appl_ID", "Contact Email"]].to_csv(
        output_dir / "email_updates.csv", index=False
    )
    return merged


# ---------------------------------------------------------------------------
# Comparison -- reporting only, changes nothing
# ---------------------------------------------------------------------------

def compare_boards(new_board: pd.DataFrame, monday_board: pd.DataFrame, output_dir: Path) -> None:
    new_keys = set(new_board["key"])
    monday_keys = set(monday_board["Name"])

    new_studies = new_board[new_board["key"].isin(new_keys - monday_keys)]
    missing_from_new = monday_board[monday_board["Name"].isin(monday_keys - new_keys)]

    logger.info("Studies in the new board but not currently on Monday (new): %d", len(new_studies))
    logger.info(
        "Studies currently on Monday but not in the new board (investigate -- removed/renamed?): %d",
        len(missing_from_new),
    )
    if len(new_studies):
        new_studies[["key"]].to_csv(output_dir / "new_studies.csv", index=False)
    if len(missing_from_new):
        missing_from_new[["Name"]].to_csv(output_dir / "missing_from_new_board.csv", index=False)
        logger.warning("Missing from new board:\n%s", missing_from_new[["Name"]].to_string(index=False))

    if "Research Focus" not in new_board.columns or "Research Focus" not in monday_board.columns:
        logger.warning("Research Focus column missing on one side -- skipping that comparison")
        return

    new_rf = (
        new_board[["key", "Research Focus"]].drop_duplicates(subset="key")
        .rename(columns={"Research Focus": "Research Focus (new)"})
    )
    old_rf = (
        monday_board[["Name", "Research Focus"]].drop_duplicates(subset="Name")
        .rename(columns={"Research Focus": "Research Focus (Monday)"})
    )
    merged_rf = pd.merge(new_rf, old_rf, left_on="key", right_on="Name", how="inner").drop(columns="Name")

    def _norm(v):
        return str(v).strip() if not pd.isna(v) else ""

    merged_rf["changed"] = merged_rf.apply(
        lambda r: _norm(r["Research Focus (new)"]) != _norm(r["Research Focus (Monday)"]), axis=1
    )
    changed = merged_rf[merged_rf["changed"]].drop(columns="changed")
    logger.info("Research Focus: %d changed, %d unchanged", len(changed), (~merged_rf["changed"]).sum())
    if len(changed):
        changed.to_csv(output_dir / "research_focus_changes.csv", index=False)
        logger.warning("Research Focus changes:\n%s", changed.to_string(index=False))


# ---------------------------------------------------------------------------
# Study type breakdown -- a standing reported metric, not just a comparison
# ---------------------------------------------------------------------------

def study_type_breakdown(final_dataset: pd.DataFrame, output_dir: Path) -> None:
    """CTN/HDP/APPLIDONLY counts on the final dataset -- written to CSV since
    this gets reported out, not just eyeballed once in the console log."""
    if "study_type" not in final_dataset.columns:
        logger.warning("study_type column not found -- skipping breakdown")
        return
    counts = final_dataset["study_type"].value_counts()
    logger.info("Study type breakdown:\n%s", counts.to_string())
    counts.rename_axis("study_type").reset_index(name="count").to_csv(
        output_dir / "study_type_breakdown.csv", index=False
    )


# ---------------------------------------------------------------------------
# Final export -- local only, never S3
# ---------------------------------------------------------------------------

def export_final(output_dir: Path, final_dataset: pd.DataFrame) -> None:
    final_dataset = final_dataset.reset_index(drop=True)
    final_dataset.index.name = "index"

    outfile = output_dir / "MondayStudiesBoard_FINAL.xlsx"
    final_dataset.to_excel(outfile, engine="xlsxwriter", index=True)
    logger.info("Final ready-to-upload file -> %s", outfile)

    n_batches = (len(final_dataset) - 1) // BATCH_SIZE + 1
    for i in range(n_batches):
        start, end = i * BATCH_SIZE, min((i + 1) * BATCH_SIZE, len(final_dataset))
        batch_file = output_dir / f"MondayStudiesBoard_FINAL_batch_{i + 1}_records_{start + 1}_to_{end}.xlsx"
        final_dataset.iloc[start:end].to_excel(batch_file, engine="xlsxwriter", index=True)
        logger.info("  batch %d (%d records) -> %s", i + 1, end - start, batch_file)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@click.command()
@click.option("--new-board", required=True, help="Local copy of the Lambda's MondayStudiesBoard_<date>.xlsx from S3.")
@click.option("--monday-export", required=True, help="Directory (or exact file) containing today's HEAL_Studies_*.xlsx export from Monday.")
@click.option("--output-dir", default=None, help="Where to write the final file + audit CSVs. Defaults to the new board's directory.")
@click.option("--debug", is_flag=True, default=False)
def main(new_board: str, monday_export: str, output_dir: str, debug: bool):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    new_board_path = Path(new_board)
    out_dir = Path(output_dir) if output_dir else new_board_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("---- STEP 1: Reading new board + live Monday export")
    new_df = read_new_board(new_board_path)
    monday_df = import_monday_board(Path(monday_export))

    logger.info("---- STEP 2: Backfilling missing emails from the live Monday board")
    backfilled = backfill_emails_from_monday(new_df, monday_df, out_dir)

    logger.info("---- STEP 3: Comparing against the live Monday board (reporting only)")
    compare_boards(backfilled, monday_df, out_dir)

    logger.info("---- STEP 4: Study type breakdown")
    study_type_breakdown(backfilled, out_dir)

    logger.info("---- STEP 5: Exporting final ready-to-upload file")
    export_final(out_dir, backfilled)


if __name__ == "__main__":
    main()
