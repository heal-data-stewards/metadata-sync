import argparse
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


def normalize_id_series(series: pd.Series) -> pd.Series:
    series = series.astype(str).str.strip()
    series = series.replace({"nan": "", "NaN": "", ".": ""})
    series = series.str.replace(r'^(\d+)\.0$', r'\1', regex=True)
    return series


def load_dataframe(path: Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".dta":
        df = pd.read_stata(path)
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path, low_memory=False, dtype=str)
    else:
        raise ValueError(f"Unsupported input file type: {path}")

    if "appl_id" in df.columns:
        df["appl_id"] = normalize_id_series(df["appl_id"])
    return df


def parse_ymd_date(value):
    if pd.isna(value):
        return pd.NaT
    if isinstance(value, (int, np.integer)):
        value = str(value).zfill(8)
    value = str(value).strip()
    if value == "" or value in {"0", "nan", "NaN", "."}:
        return pd.NaT

    for fmt in ["%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%Y%m%d%H%M%S"]:
        try:
            return pd.to_datetime(value, format=fmt, errors="raise")
        except (ValueError, TypeError):
            continue
    return pd.to_datetime(value, errors="coerce")


def is_empty_series(series: pd.Series) -> pd.Series:
    return series.isin(["0", ""]) | series.isna()


def clean_string_columns(df: pd.DataFrame, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"nan": "", "NaN": ""})
    return df


def load_manual_matches(path: Path, sheet_name: str = "matches_2026-03-23") -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path, sheet_name=sheet_name, dtype=str, engine="openpyxl")
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path, low_memory=False, dtype=str)
    else:
        raise ValueError(f"Unsupported manual matches file type: {path}")

    if "appl_id" in df.columns:
        df["appl_id"] = normalize_id_series(df["appl_id"])
    return df


def group_ids(df: pd.DataFrame, cols, name: str) -> pd.Series:
    return df.groupby(cols, dropna=False).ngroup().add(1).astype("Int64")


def build_study_lookup_table(
    mysql_df: pd.DataFrame,
    reporter_dqaudit_df: pd.DataFrame,
    manual_matches_df: Optional[pd.DataFrame] = None,
    export_debug: Optional[Path] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Recreate HEAL_04_StudyTable.do logic in Python."""
    mysql_df = mysql_df.copy()
    mysql_df.columns = [c.strip() for c in mysql_df.columns]
    # Step 1: Prep data
    print(mysql_df.columns)
    mysql_df = mysql_df.drop(columns=[c for c in mysql_df.columns if c == "Unnamed: 0"], errors="ignore")
    print(f"Number of entries in mysql table: {len(mysql_df)}") #2521
    mysql_df = mysql_df[~is_empty_series(mysql_df.get("appl_id", pd.Series(dtype=str)))].copy()
    print(f"Number of entries in mysql table removing empty appl_id: {len(mysql_df)}") #2468
    mysql_df["appl_id"] = mysql_df["appl_id"].astype(str)

    # Keep appl_ids from MySQL that are present in the reporter/awards table.
    appls_in_reporter = (
        mysql_df[["appl_id"]].drop_duplicates().assign(in_reporter_table=1)
    )
    print(f"Number of appls in reporter after dropping duplicates: {len(appls_in_reporter)}") ## Match till here : 2444

    # Reporter DQ audit rows NOT already in the reporter table
    reporter_dqaudit_df = reporter_dqaudit_df.copy()
    reporter_dqaudit_df["appl_id"] = reporter_dqaudit_df["appl_id"].astype(str)
    reporter_dqaudit_df = reporter_dqaudit_df[~reporter_dqaudit_df["appl_id"].isin(appls_in_reporter["appl_id"])]
    print(f"Number of reporter dqaudit records pulled out: {len(reporter_dqaudit_df)}") ## Match 489
    if "fisc_yr" in reporter_dqaudit_df.columns:
        print(f"Taking care of the fisc_year column in reporter_dqaudit") ## Exists
        reporter_dqaudit_df["fisc_yr"] = pd.to_numeric(reporter_dqaudit_df["fisc_yr"], errors="coerce").astype("Int64")
    else:
        print(f"Warning - No fisc_yr column found in the reporter_dqaudit table")

    mysql_df = mysql_df[mysql_df["entity_type"].fillna("") != "Other"].copy()
    print(f"Number of records in mysql table after removing Other entity Types: {len(mysql_df)}") # Match 2462
    mysql_df["in_mysql"] = 1

    mysql_df["xhas_subproj_num"] = mysql_df["subproj_id"].apply(
        lambda v: 1 if pd.notna(v) and str(v) != "" else np.nan
    )
    mysql_df["has_subproj_num_by_sernum"] = (
        mysql_df.groupby("proj_ser_num")["xhas_subproj_num"].transform("max")
    )
    if export_debug is not None:
        mysql_df.to_csv(export_debug/"mysql_w_has_subproject.csv", index=False)
    mysql_df.drop(columns=["xhas_subproj_num"], inplace=True)

    combined_df = pd.concat([mysql_df, reporter_dqaudit_df], ignore_index=True, sort=False)
    print(f"Number of entries in the combined dataset: {len(combined_df)}") # Match: 2951
    combined_df["in_mysql"] = combined_df["in_mysql"].fillna(0).astype(int)
    combined_df["appl_id"] = combined_df["appl_id"].astype(str)

    stewards_id_vars = ["proj_ser_num", "subproj_id", "proj_num_spl_sfx_code"]
    combined_df = clean_string_columns(combined_df, stewards_id_vars)

    combined_df["xstudy_id_stewards"] = group_ids(combined_df, stewards_id_vars, "xstudy_id_stewards")

    has_hdp = combined_df["hdp_id"].notna() & (combined_df["hdp_id"].astype(str) != "")
    combined_df["study_id"] = group_ids(combined_df[has_hdp], ["xstudy_id_stewards", "hdp_id"], "study_id").reindex(combined_df.index)
    combined_df["study_id"] = combined_df["study_id"].astype("Int64")
    combined_df = combined_df[["study_id", "xstudy_id_stewards"] + [c for c in combined_df.columns if c not in {"study_id", "xstudy_id_stewards"}]]
    if export_debug is not None:
        combined_df.to_csv(export_debug/"mysql_today_withstudyid.csv", index=False)

    # Stata lines 128-132: QC check — rows missing study_id but with a valid xstudy_id_stewards
    check_studyid = combined_df[combined_df["study_id"].isna() & combined_df["xstudy_id_stewards"].notna()]
    if export_debug is not None:
        check_studyid.to_csv(export_debug/"check_studyid_assigns.csv", index=False)
    print(f"Rows missing study_id but with xstudy_id_stewards: {len(check_studyid)}") # Match 1319

    # Counts for QC and later selection logic
    sis_count = (
        combined_df[["xstudy_id_stewards", "appl_id"]]
        .drop_duplicates()
        .groupby("xstudy_id_stewards", dropna=False)
        .size()
        .reset_index(name="num_appl_by_xstudyidstewards")
    )
    if export_debug is not None:
        sis_count.to_csv(export_debug/"sis_count.csv", index=False)

    hdp_cols = (
        combined_df[["xstudy_id_stewards", "hdp_id", "archived"]]
        .copy()
        .drop_duplicates()
    )
    print(f"Number of rows in HDP Cols is {len(hdp_cols)}")
    hdp_cols["hdp_id_cnt"] = hdp_cols["hdp_id"].replace({"": pd.NA}).notna().astype(int)
    hdpid_count = (
        hdp_cols.groupby("xstudy_id_stewards", dropna=False)
        .agg(
            num_hdp_by_xstudyidstewards=("hdp_id_cnt", "sum"),
            num_live_hdps=("archived", lambda x: (x == "live").sum()),
            num_arch_hdps=("archived", lambda x: (x == "archived").sum()),
        )
        .reset_index()
    )
    if export_debug is not None:
        hdpid_count.to_csv(export_debug/"hdpid_count.csv", index=False)
    print(f"Number of rows in hdpid_count is: {len(hdpid_count)}")
    combined_df = combined_df.merge(sis_count, on="xstudy_id_stewards", how="left")
    combined_df = combined_df.merge(hdpid_count, on="xstudy_id_stewards", how="left")
    # Step 1h: Create compound_key (concatenation of appl_id and hdp_id)
    # fillna BEFORE astype(str) — otherwise NaN becomes the string "nan"
    combined_df["compound_key"] = combined_df["appl_id"].fillna("").astype(str) + "_" + combined_df["hdp_id"].fillna("").astype(str)

    print(f"Does the input table already have num_hdp_by_appl? {'num_hdp_by_appl' in combined_df.columns}")
 
    if "num_hdp_by_appl" not in combined_df.columns:
        combined_df["num_hdp_by_appl"] = 0
    
    combined_df["num_hdp_by_appl"] = pd.to_numeric(combined_df["num_hdp_by_appl"], errors="coerce").astype("Int64").fillna(0)

    mysql_hasappls_df = combined_df.copy()
    if export_debug is not None:
        mysql_hasappls_df.to_csv(export_debug/"mysql_hasappls_today.csv", index=False)
    
    # QC check flags
    qc = mysql_hasappls_df.copy()
    qc["valid_flag"] = 0
    qc.loc[qc["num_hdp_by_xstudyidstewards"].isin([0, 1]), "valid_flag"] = 1
    print(f"Number of valid flag entries: {len(qc[qc['valid_flag']==1])}")
    qc.loc[
        (qc["num_appl_by_xstudyidstewards"] == qc["num_hdp_by_xstudyidstewards"]) &
        (qc["num_hdp_by_appl"] == 1),
        "valid_flag",
    ] = 1
    print(f"Number of valid flag entries: {len(qc[qc['valid_flag']==1])}")
    qc.loc[qc["num_appl_by_xstudyidstewards"] == 1, "valid_flag"] = 1
    print(f"Number of valid flag entries: {len(qc[qc['valid_flag']==1])}")
    qc_issues = qc[qc["valid_flag"] == 0].copy()

    if export_debug is not None:
        print(f"Number of QC Issues: {len(qc_issues)}") ## MATCH: 358 ROWS.
        qc_issues.to_csv(export_debug / "sis_hdpid_comparison_issues.csv", index=False)

    ### STEP 2
    hdpid0 = mysql_hasappls_df[mysql_hasappls_df["num_hdp_by_xstudyidstewards"] == 0].copy()
    print(f"Number of hdpid0 records: {len(hdpid0)}") # Match 244
    hdpid1 = mysql_hasappls_df[mysql_hasappls_df["num_hdp_by_xstudyidstewards"] == 1].copy()
    print(f"Number of hdpid0 records: {len(hdpid1)}") # Match 2254

    studyidgood1 = mysql_hasappls_df[
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 0) &
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 1) &
        (mysql_hasappls_df["num_appl_by_xstudyidstewards"] == mysql_hasappls_df["num_hdp_by_xstudyidstewards"]) &
        (mysql_hasappls_df["num_hdp_by_appl"] == 1)
    ][["study_id", "appl_id", "hdp_id", "compound_key"]].copy()
    print(f"StudyIDGood1: {len(studyidgood1)}") #Match 86

    studyidgood2 = mysql_hasappls_df[
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 0) &
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 1) &
        (mysql_hasappls_df["num_appl_by_xstudyidstewards"] == 1)
    ][["study_id", "appl_id", "hdp_id", "compound_key"]].copy()
    print(f"StudyIDGood2: {len(studyidgood2)}") # Match 9
    
    studyidbad = mysql_hasappls_df[
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 0) &
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 1) &
        ~(
            (mysql_hasappls_df["num_appl_by_xstudyidstewards"] == mysql_hasappls_df["num_hdp_by_xstudyidstewards"]) &
            (mysql_hasappls_df["num_hdp_by_appl"] == 1)
        ) &
        (mysql_hasappls_df["num_appl_by_xstudyidstewards"] != 1)
    ].copy()
    print(f"StudyIDBad: {len(studyidbad)}") # Match 358

    # Step 3: assign new study IDs for hdpid0
    maxid = mysql_hasappls_df["study_id"].max()
    if pd.isna(maxid):
        maxid = 0
    else:
        maxid = int(maxid)

    hdpid0 = hdpid0.sort_values("xstudy_id_stewards").copy()
    hdpid0["tempn"] = group_ids(hdpid0, ["xstudy_id_stewards"], "tempn")
    hdpid0["study_id"] = hdpid0["tempn"].add(maxid)
    studyidgood3 = hdpid0[["study_id", "appl_id", "hdp_id", "compound_key", "in_mysql"]].copy()
    print(f"StudyIDGood3: {len(studyidgood3)}") # Match 244

    # Step 4: update hdpid1
    studyid_sis_key = (
        hdpid1[["study_id", "xstudy_id_stewards"]]
        .dropna(subset=["study_id"])
        .drop_duplicates(subset=["xstudy_id_stewards"])  # enforce m:1 like Stata's merge m:1
        .copy()
    )
    studyid_sis_key = studyid_sis_key.rename(columns={"study_id": "xstudy_id"})
    hdpid1 = hdpid1.merge(studyid_sis_key, on="xstudy_id_stewards", how="left")
    hdpid1.loc[hdpid1["study_id"].isna(), "study_id"] = hdpid1.loc[hdpid1["study_id"].isna(), "xstudy_id"]
    studyidgood4 = hdpid1[["study_id", "appl_id", "hdp_id", "compound_key", "in_mysql"]].copy()
    print(f"StudyIDGood4: {len(studyidgood4)}") # Match 9

    # Step 5: handle studyidbad
    bad_nonmiss = studyidbad[studyidbad["hdp_id"].notna() & (studyidbad["hdp_id"].astype(str) != "")].copy()
    bad_nonmiss = bad_nonmiss.rename(columns={
        "study_id": "zstudy_id",
        "appl_id": "zappl_id",
        "proj_ser_num": "zproj_ser_num",
        "proj_num_spl_ty_code": "zproj_num_spl_ty_code",
        "proj_num_spl_sfx_code": "zproj_num_spl_sfx_code",
        "act_code": "zact_code",
        "hdp_id": "zhdp_id",
        "archived": "zarchived",
        "proj_title": "zproj_title",
        "bgt_end_date": "zbgt_end_date",
        "bgt_strt_date": "zbgt_strt_date",
        "in_mysql": "zin_mysql",
    })
    print(f"Number of records in bad_nomiss: {len(bad_nonmiss)}") #Match: 199

    bad_missing = studyidbad[~(studyidbad["hdp_id"].notna() & (studyidbad["hdp_id"].astype(str) != ""))].copy()
    print(f"Number of records in bad_missing: {len(bad_missing)}") #Match: 159

    # Match Stata's explicit keep before joinby: only join key + z-prefixed columns
    nonmiss_merge_cols = ["xstudy_id_stewards"] + [c for c in bad_nonmiss.columns if c.startswith("z")]
    if not bad_missing.empty:
        bad_joined = bad_missing.merge(
            bad_nonmiss[nonmiss_merge_cols],
            on="xstudy_id_stewards",
            how="inner",
        )
    else:
        bad_joined = pd.DataFrame(columns=list(bad_missing.columns) + [c for c in nonmiss_merge_cols if c != "xstudy_id_stewards"])

    def _str_eq(a: pd.Series, b: pd.Series) -> pd.Series:
        """String equality that treats NaN==NaN as True, matching Stata's ''=='' behavior."""
        return (a == b) | (a.isna() & b.isna())

    bad_joined["act_code_match"] = _str_eq(bad_joined["act_code"], bad_joined["zact_code"]).astype(int)
    bad_joined["typ_match"] = _str_eq(bad_joined["proj_num_spl_ty_code"], bad_joined["zproj_num_spl_ty_code"]).astype(int)
    def _norm_title(s: pd.Series) -> pd.Series:
        return s.str.strip().str.replace(r'\s+', ' ', regex=True)

    bad_joined["title_match"] = _str_eq(_norm_title(bad_joined["proj_title"]), _norm_title(bad_joined["zproj_title"])).astype(int)

    bad_joined["count_actcode_matches"] = (
        bad_joined.groupby("appl_id", dropna=False)["act_code_match"].transform("sum")
    )
    bad_joined["count_typ_matches"] = (
        bad_joined.groupby("appl_id", dropna=False)["typ_match"].transform("sum")
    )

    bad_joined["xappl_pairs"] = (
        bad_joined.groupby(["appl_id", "zappl_id"], dropna=False)
        .ngroup()
        .add(1)
    )
    bad_joined["tag"] = (~bad_joined.duplicated(subset=["xstudy_id_stewards", "xappl_pairs"]) ).astype(int)
    bad_joined["num_appl_pairs"] = (
        bad_joined.groupby("xstudy_id_stewards", dropna=False)["tag"].transform("sum")
    )

    bad_joined = bad_joined.sort_values(["xstudy_id_stewards", "zarchived", "zhdp_id", "appl_id"])
    if export_debug is not None:
        print(f"Number of records in xstudyidgood5: {len(bad_joined)}") #Match, 354
        bad_joined.to_csv(export_debug / "xstudyidgood5.csv", index=False)

    xstudyidgood5a = bad_joined[
        (bad_joined["count_actcode_matches"] == 1) &
        (bad_joined["act_code_match"] == 1)
    ].copy()
    xstudyidgood5a["study_id"] = xstudyidgood5a["zstudy_id"].astype("Int64")
    xstudyidgood5a = xstudyidgood5a[["study_id", "appl_id", "hdp_id", "compound_key"]]
    print(f"Number of records in xstudyidgood5a: {len(xstudyidgood5a)}") #Match 56

    xstudyidgood5b = bad_joined[
        (bad_joined["count_actcode_matches"] != 1) &
        (bad_joined["num_appl_pairs"] == 1)
    ].copy()
    xstudyidgood5b["study_id"] = xstudyidgood5b["zstudy_id"].astype("Int64")
    xstudyidgood5b = xstudyidgood5b[["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"]]
    print(f"Number of records in xstudyidgood5b: {len(xstudyidgood5b)}") #Match 10

    xstudyidgood5c = bad_joined[
        (bad_joined["count_actcode_matches"] != 1) &
        (bad_joined["num_appl_pairs"] != 1) &
        (bad_joined["num_live_hdps"] == 1) &
        (bad_joined["zarchived"] == "live")
    ].copy()
    xstudyidgood5c["study_id"] = xstudyidgood5c["zstudy_id"].astype("Int64")
    xstudyidgood5c = xstudyidgood5c[["study_id", "appl_id", "hdp_id", "compound_key"]]
    print(f"Number of records in xstudyidgood5c: {len(xstudyidgood5c)}")

    xstudyidgood5d = bad_joined[
        (bad_joined["count_actcode_matches"] != 1) &
        (bad_joined["num_appl_pairs"] != 1) &
        (bad_joined["num_live_hdps"] == 0)
    ].copy()
    print(f"Intermediate length of xstudyidgood5d: {len(xstudyidgood5d)}")
    if not xstudyidgood5d.empty:
        xstudyidgood5d["gap"] = xstudyidgood5d["bgt_strt_date"].apply(parse_ymd_date) - xstudyidgood5d["zbgt_end_date"].apply(parse_ymd_date)
        xstudyidgood5d["gap"] = xstudyidgood5d["gap"].where(xstudyidgood5d["gap"] >= pd.Timedelta(0), pd.NaT)
        xstudyidgood5d["group"] = (
            xstudyidgood5d.groupby(["xstudy_id_stewards", "appl_id"], dropna=False).ngroup().add(1)
        )
        xstudyidgood5d["min_gap"] = xstudyidgood5d.groupby("group")["gap"].transform("min")
        # Stata: keep if gap==min_gap treats missing==missing as True; NaT==NaT is False in pandas
        both_nat = xstudyidgood5d["gap"].isna() & xstudyidgood5d["min_gap"].isna()
        xstudyidgood5d = xstudyidgood5d[(xstudyidgood5d["gap"] == xstudyidgood5d["min_gap"]) | both_nat].copy()
        print(f"After gap==min_gap filter: {len(xstudyidgood5d)}") # Stata: 127, Match
        xstudyidgood5d["match"] = ((~xstudyidgood5d["min_gap"].isna()) | (xstudyidgood5d["title_match"] == 1)).astype(int)
        xstudyidgood5d["any_match"] = xstudyidgood5d.groupby("appl_id")["match"].transform("max")
        xstudyidgood5d = xstudyidgood5d[~((xstudyidgood5d["match"] == 0) & (xstudyidgood5d["any_match"] == 1))].copy()
        xstudyidgood5d["rowcount"] = xstudyidgood5d.groupby("appl_id")["appl_id"].transform("size")
        print(f"Number of records in xstudyidgood5d: {len(xstudyidgood5d)}")

        studyidgood5d_1 = xstudyidgood5d[xstudyidgood5d["rowcount"] == 1].copy()
        studyidgood5d_1["study_id"] = studyidgood5d_1["zstudy_id"].astype("Int64")
        studyidgood5d_1 = studyidgood5d_1[["study_id", "appl_id", "hdp_id", "compound_key"]]

        studyidgood5d_2 = xstudyidgood5d[
            (xstudyidgood5d["rowcount"] > 1) &
            (xstudyidgood5d["any_match"] == 1)
        ].copy()
        studyidgood5d_2["study_id"] = studyidgood5d_2["zstudy_id"].astype("Int64")
        studyidgood5d_2 = studyidgood5d_2[["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"]]

        nomatches5d = xstudyidgood5d[(xstudyidgood5d["rowcount"] > 1) & (xstudyidgood5d["any_match"] == 0)].copy()
    else:
        studyidgood5d_1 = pd.DataFrame(columns=["study_id", "appl_id", "hdp_id", "compound_key"])
        studyidgood5d_2 = pd.DataFrame(columns=["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"])
        nomatches5d = pd.DataFrame(columns=bad_joined.columns)

    if export_debug is not None:
        studyidgood5d_1.to_csv(export_debug / "studyidgood5d_1.csv", index=False)
        print(f"Number of records in xstudyidgood5d_1: {len(studyidgood5d_1)}") # Match 33

        studyidgood5d_2.to_csv(export_debug / "studyidgood5d_2.csv", index=False)
        print(f"Number of records in xstudyidgood5d_2: {len(studyidgood5d_2)}") # Match 30

        print(f"Number of records in nomatches5d: {len(nomatches5d)}") #Match 51

    # Step 5e: generate manual-match template (Stata lines 406-416)
    # rows from bad_joined not resolved by 5a/5b/5c/5d: num_live_hdps > 1
    unresolved_5e = bad_joined[
        (bad_joined["count_actcode_matches"] != 1) &
        (bad_joined["num_appl_pairs"] != 1) &
        ~bad_joined["num_live_hdps"].isin([0, 1])
    ].copy()
    print(f"The number of records in the final unresolved {len(unresolved_5e)}")
    manual_template = pd.concat([unresolved_5e, nomatches5d], ignore_index=True, sort=False)
    manual_template = manual_template.sort_values(
        ["xstudy_id_stewards", "bgt_strt_date", "appl_id", "zappl_id"],
        na_position="last",
    )
    _template_cols = [
        "study_id", "xstudy_id_stewards", "appl_id", "hdp_id", "archived",
        "num_appl_by_xstudyidstewards", "num_hdp_by_appl", "num_hdp_by_xstudyidstewards", "compound_key",
        "proj_ser_num", "subproj_id", "proj_num_spl_sfx_code", "proj_abs", "act_code",
        "fund_ic", "ic_fund_code", "ic_fund_yr", "awd_not_date",
        "bgt_end", "bgt_strt", "fisc_yr", "fund_mech", "phr_text",
        "proj_end_date", "proj_num", "proj_strt_date", "proj_title", "res_prg",
        "guid_type", "study_name", "project_num", "project_title",
        "zstudy_id", "zappl_id", "zhdp_id", "zarchived", "zproj_ser_num", "zproj_num_spl_sfx_code",
        "zact_code", "zbgt_end_date", "zbgt_strt_date", "zproj_num_spl_ty_code", "zproj_title", "zin_mysql",
        "act_code_match", "typ_match", "title_match",
        "count_actcode_matches", "count_typ_matches", "xappl_pairs", "tag", "num_appl_pairs",
    ]
    manual_template = manual_template[[c for c in _template_cols if c in manual_template.columns]]
    print(f"Manual match template rows: {len(manual_template)}") # Stata: ~99

    if export_debug is not None:
        nomatches5d.to_csv(export_debug / "nomatches5d.csv", index=False)
        manual_template.to_csv(export_debug / "xstudy_manual_matches.csv", index=False)

    if manual_matches_df is None:
        raise ValueError(
            "Manual match file is required to complete full study_id assignment for the unmatched studyidbad records. "
            "Provide --manual-matches <path> to continue."
        )

    manual = manual_matches_df.copy()
    manual_columns = [c for c in [
        "study_id", "xstudy_id_stewards", "zstudy_id", "match",
        "appl_id", "zappl_id", "zhdp_id", "compound_key",
    ] if c in manual.columns]
    manual = manual[manual_columns].astype(str)
    manual["match"] = manual["match"].replace({"nan": "0", "NaN": "0"}).fillna("0").astype(int)
    manual["study_id"] = pd.to_numeric(manual["study_id"], errors="coerce").astype("Int64")
    manual["zstudy_id"] = pd.to_numeric(manual["zstudy_id"], errors="coerce").astype("Int64")
    manual["xstudy_id_stewards"] = pd.to_numeric(manual["xstudy_id_stewards"], errors="coerce").astype("Int64")

    # hdp_id for these records is empty (they came from bad_missing with no hdp_id)
    if "zhdp_id" in manual.columns:
        manual["hdp_id"] = manual["zhdp_id"]
    else:
        manual["hdp_id"] = ""

    # Preserve compound_key from the template (bad_missing side); only reconstruct if absent
    if "compound_key" not in manual.columns or manual["compound_key"].isin(["nan", "", "NaN"]).all():
        manual["compound_key"] = manual["appl_id"].astype(str) + "_"

    manual = manual[manual["appl_id"].notna() & (manual["appl_id"] != "")].copy()
    print(f"  [5e diag] rows after appl_id filter: {len(manual)}")
    print(f"  [5e diag] match value counts:\n{manual['match'].value_counts(dropna=False)}")
    manual["any_match"] = manual.groupby("appl_id")["match"].transform("max")
    print(f"  [5e diag] any_match value counts:\n{manual['any_match'].value_counts(dropna=False)}")
    n_before = len(manual)
    manual = manual[~((manual["match"] == 0) & (manual["any_match"] == 1))].copy()
    print(f"  [5e diag] rows dropped by match==0 & any_match==1: {n_before - len(manual)} (remaining: {len(manual)})")
    manual["rowcount"] = manual.groupby("appl_id")["appl_id"].transform("size")
    print(f"  [5e diag] rowcount distribution:\n{manual['rowcount'].value_counts().sort_index()}")
    print(f"Number of records in matches5e: {len(manual)}") #Match 89

    studyidgood5e_1 = manual[manual["rowcount"] == 1].copy()
    studyidgood5e_1["study_id"] = studyidgood5e_1["zstudy_id"].astype("Int64")
    studyidgood5e_1 = studyidgood5e_1[["study_id", "appl_id", "hdp_id", "compound_key"]].copy()
    print(f"Number of records in studyidgood5e_1: {len(studyidgood5e_1)}") #Match 2

    # Stata: keep if any_match==0 (no rowcount filter), then drop z*, duplicates drop
    maxid2 = studyidgood3["study_id"].max()
    if pd.isna(maxid2):
        maxid2 = 0
    else:
        maxid2 = int(maxid2)
    # Stata: drop z*, duplicates drop → hdp_id is the original empty value from bad_missing
    # Using zhdp_id (set earlier) gives different hdp_ids per z-row, preventing dedup
    studyidgood5e_2 = (
        manual[manual["any_match"] == 0][["xstudy_id_stewards", "appl_id"]]
        .drop_duplicates()
        .sort_values("xstudy_id_stewards")
        .copy()
    )
    studyidgood5e_2["hdp_id"] = ""
    studyidgood5e_2["compound_key"] = studyidgood5e_2["appl_id"].astype(str) + "_"
    studyidgood5e_2["tempn"] = group_ids(studyidgood5e_2, ["xstudy_id_stewards"], "tempn")
    studyidgood5e_2["study_id"] = studyidgood5e_2["tempn"].add(maxid2)
    studyidgood5e_2 = studyidgood5e_2[["study_id", "appl_id", "hdp_id", "compound_key"]].copy()
    print(f"Number of records in studyidgood5e_2: {len(studyidgood5e_2)}") #Match 24


    studyidgood5e_3 = manual[(manual["rowcount"] > 1) & (manual["any_match"] == 1)].copy()
    studyidgood5e_3["study_id"] = studyidgood5e_3["zstudy_id"].astype("Int64")
    studyidgood5e_3 = studyidgood5e_3[["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"]].copy()
    print(f"Number of records in studyidgood5e_3: {len(studyidgood5e_3)}") #Match 35

    studyidgood5 = pd.concat(
        [
            xstudyidgood5a,
            xstudyidgood5c,
            studyidgood5d_1,
            studyidgood5e_1,
            studyidgood5e_2,
        ],
        ignore_index=True,
        sort=False,
    )
    print(f"Final Number of records in study id good5: {len(studyidgood5)}")

    studyidmulti = pd.concat(
        [
            xstudyidgood5b,
            studyidgood5e_3,
            studyidgood5d_2,
        ],
        ignore_index=True,
        sort=False,
    )
    print(f"Final Number of records in study id good5: {len(studyidmulti)}") #Match 75

    studyidgood6 = bad_nonmiss[["zstudy_id", "zappl_id", "zhdp_id", "compound_key"]].copy()
    studyidgood6.columns = ["study_id", "appl_id", "hdp_id", "compound_key"]
    print(f"Final Number of records in studyidgood6: {len(studyidgood6)}") # Match 199

    studyidkey = pd.concat(
        [
            studyidgood1,
            studyidgood2[["study_id", "appl_id", "hdp_id", "compound_key"]],
            studyidgood3[["study_id", "appl_id", "hdp_id", "compound_key"]],
            studyidgood4[["study_id", "appl_id", "hdp_id", "compound_key"]],
            studyidgood6,
            studyidgood5,
        ],
        ignore_index=True,
        sort=False,
    )
    print(f"Number of records in StudyIDKey: {len(studyidkey)}") # Match 2918

    studyidkey = studyidkey.drop_duplicates(subset=["compound_key"], keep="first")
    studyidkey = studyidkey.rename(columns={"study_id": "study_id_final"})
    print(f"Number of records in StudyIDKey: {len(studyidkey)}")

    mysql_studyid = mysql_hasappls_df.copy()
    mysql_studyid = mysql_studyid.merge(
        studyidkey[["compound_key", "study_id_final"]],
        on="compound_key",
        how="left",
    )
    mysql_studyid = mysql_studyid.sort_values(["study_id_final", "appl_id", "hdp_id"])
    mysql_studyid = mysql_studyid.drop(columns=["study_id", "xstudy_id_stewards"], errors="ignore")
    print(f"Number of records in mysql_studyid: {len(mysql_studyid)}") # Match 2951

    if not studyidmulti.empty:
        studyidmulti = studyidmulti.rename(columns={"study_id": "xstudy_id_final"})
        mysql_studyid = mysql_studyid.merge(
            studyidmulti[["compound_key", "xstudy_id_final"]],
            on="compound_key",
            how="left",
        )
        missing_final = mysql_studyid["study_id_final"].isna() & mysql_studyid["xstudy_id_final"].notna()
        mysql_studyid.loc[missing_final, "study_id_final"] = mysql_studyid.loc[missing_final, "xstudy_id_final"]
        mysql_studyid.drop(columns=["xstudy_id_final"], inplace=True)

    if export_debug is not None:
        print(f"Number of records in mysql_study at the end of Step 6: {len(mysql_studyid)}") #Match 2993
        mysql_studyid.to_csv(export_debug / "mysql_studyid.csv", index=False)

    # Step 7: Most recent appl_id for each study
    recent = mysql_studyid.copy()
    print(f"  [step7 diag] mysql_studyid rows: {len(recent)}, NaN study_id_final: {recent['study_id_final'].isna().sum()}")
    recent = recent[recent["study_id_final"].notna()].copy()

    # Stata computes latest_proj_end_dt_forstudy as a label only — no filter on it
    recent["proj_end_date_date"] = recent["proj_end_date"].apply(parse_ymd_date)
    recent["latest_proj_end_dt_forstudy"] = (
        recent.groupby("study_id_final")["proj_end_date_date"].transform("max")
    )

    # Filter 1: latest fiscal year
    recent["fisc_yr"] = pd.to_numeric(recent["fisc_yr"], errors="coerce").astype("Int64")
    recent["latest_fy"] = recent.groupby("study_id_final")["fisc_yr"].transform("max")
    fy_keep = (recent["latest_fy"] == recent["fisc_yr"]) | (recent["fisc_yr"].isna() & recent["latest_fy"].isna())
    recent = recent[fy_keep].copy()
    print(f"  [step7 diag] after latest_fy filter: {len(recent)}")

    # Filter 2: latest budget end date
    recent["bgt_end_date_parsed"] = recent["bgt_end_date"].apply(parse_ymd_date)
    recent["latest_bgt_end"] = recent.groupby("study_id_final")["bgt_end_date_parsed"].transform("max")
    recent = recent[(recent["latest_bgt_end"] == recent["bgt_end_date_parsed"]) | (recent["bgt_end_date_parsed"].isna() & recent["latest_bgt_end"].isna())].copy()
    print(f"  [step7 diag] after latest_bgt_end filter: {len(recent)}")

    # Filter 3: latest award notice date (Stata re-parses from raw string, taking first 10 chars)
    recent["awd_not_date_date"] = recent["awd_not_date"].apply(parse_ymd_date)
    recent["latest_awd_not_date"] = recent.groupby("study_id_final")["awd_not_date_date"].transform("max")
    recent = recent[(recent["latest_awd_not_date"] == recent["awd_not_date_date"]) | (recent["awd_not_date_date"].isna() & recent["latest_awd_not_date"].isna())].copy()
    print(f"  [step7 diag] after latest_awd_not_date filter: {len(recent)}")

    mostrecentapplid = recent[["study_id_final", "appl_id"]].rename(columns={"appl_id": "study_most_recent_appl"}).copy()
    print(f"Number of records in mostrecentapplid: {len(mostrecentapplid)}") # Match with actual Stata output: 1801 records.


    # Step 8: First appl_id for each study
    first = mysql_studyid.copy()
    first = first[first["study_id_final"].notna()].copy()

    # Stata computes earliest_proj_strt_dt_forstudy as a label only — no filter on it
    first["proj_strt_date_date"] = first["proj_strt_date"].apply(parse_ymd_date)
    first["earliest_proj_strt_dt_forstudy"] = first.groupby("study_id_final")["proj_strt_date_date"].transform("min")

    # Filter 1: earliest fiscal year
    first["fisc_yr"] = pd.to_numeric(first["fisc_yr"], errors="coerce").astype("Int64")
    first["first_fy"] = first.groupby("study_id_final")["fisc_yr"].transform("min")
    first = first[(first["first_fy"] == first["fisc_yr"]) | (first["fisc_yr"].isna() & first["first_fy"].isna())].copy()

    # Filter 2: earliest budget start date
    first["bgt_strt_date_parsed"] = first["bgt_strt_date"].apply(parse_ymd_date)
    first["earliest_bgt_strt"] = first.groupby("study_id_final")["bgt_strt_date_parsed"].transform("min")
    first = first[(first["earliest_bgt_strt"] == first["bgt_strt_date_parsed"]) | (first["bgt_strt_date_parsed"].isna() & first["earliest_bgt_strt"].isna())].copy()

    # Filter 3: earliest award notice date
    first["awd_not_date_date"] = first["awd_not_date"].apply(parse_ymd_date)
    first["earliest_awd_not_date"] = first.groupby("study_id_final")["awd_not_date_date"].transform("min")
    first = first[(first["earliest_awd_not_date"] == first["awd_not_date_date"]) | (first["awd_not_date_date"].isna() & first["earliest_awd_not_date"].isna())].copy()

    firstapplid = first[["study_id_final", "appl_id"]].rename(columns={"appl_id": "study_first_appl"}).copy()
    print(f"Number of records in firstapplid: {len(firstapplid)}") #


    # Step 9: HDP/applications mapping
    hdpapplid = mysql_studyid[
        (mysql_studyid["study_id_final"].notna()) &
        (mysql_studyid["hdp_id"].notna()) &
        (mysql_studyid["hdp_id"].astype(str) != "")
    ][["study_id_final", "hdp_id", "appl_id"]].copy()
    hdpapplid = hdpapplid.rename(columns={"appl_id": "study_hdp_id_appl", "hdp_id": "study_hdp_id"})
    print(f"Number of records in hdpapplid: {len(hdpapplid)}") #

    # Step 10: Create study_lookup_table
    study_table = mysql_studyid[
        mysql_studyid["study_id_final"].notna()
    ][["study_id_final", "appl_id", "in_mysql", "compound_key"]].copy()
    study_table = study_table.rename(columns={"in_mysql": "appl_id_in_repawd"})

    study_table = study_table.merge(hdpapplid, on="study_id_final", how="left")
    study_table = study_table.merge(mostrecentapplid, on="study_id_final", how="left")
    study_table = study_table.merge(firstapplid, on="study_id_final", how="left")

    study_table = study_table.merge(
        mysql_df[["compound_key", "entity_type", "res_net"]],
        on="compound_key",
        how="left",
        suffixes=(None, "_orig"),
    )

    study_table["study"] = (study_table["entity_type"] == "Study").astype(int)
    study_table["ctn"] = (study_table["entity_type"] == "CTN").astype(int)
    study_table["any_study"] = study_table.groupby("study_id_final")["study"].transform("max")
    study_table["any_ctn"] = study_table.groupby("study_id_final")["ctn"].transform("max")

    # Drop groups where the study is CTN-only
    study_table = study_table[~((study_table["any_ctn"] == 1) & (study_table["any_study"] != 1))].copy()

    # Drop studies where every appl_id is from DQ audit only
    study_table["max_flag"] = study_table.groupby("study_id_final")["appl_id_in_repawd"].transform("max")
    study_table = study_table[study_table["max_flag"] != 0].copy()
    study_table.drop(columns=["max_flag"], inplace=True)

    study_table = study_table.rename(columns={"study_id_final": "xstudy_id"})
    study_table["xstudy_id"] = study_table["xstudy_id"].astype("Int64").astype(str)

    output_columns = [
        "appl_id",
        "xstudy_id",
        "study_hdp_id",
        "study_hdp_id_appl",
        "study_most_recent_appl",
        "study_first_appl",
        "compound_key",
    ]
    study_table = study_table[output_columns]
    print(f"Number of entries in the Final study lookup table: {len(study_table)}")
    
    study_table_dd = pd.DataFrame(
        [
            {
                "table_name": "study_lookup_table",
                "var_name": "appl_id",
                "var_label": "Application ID",
                "var_fmt": "VARCHAR(8)",
                "var_length": "8",
                "identifier": "",
                "var_note": "",
            },
            {
                "table_name": "study_lookup_table",
                "var_name": "xstudy_id",
                "var_label": "Study ID",
                "var_fmt": "VARCHAR(4)",
                "var_length": "4",
                "identifier": "",
                "var_note": "The study ID is generated by HEAL Stewards",
            },
            {
                "table_name": "study_lookup_table",
                "var_name": "study_hdp_id",
                "var_label": "The study's hdp_id",
                "var_fmt": "CHAR(8)",
                "var_length": "8",
                "identifier": "FK",
                "var_note": "There cannot be more than 1 HDP ID for a given Study ID, by definition",
            },
            {
                "table_name": "study_lookup_table",
                "var_name": "study_hdp_id_appl",
                "var_label": "The appl_id of the study's hdp_id",
                "var_fmt": "VARCHAR(8)",
                "var_length": "8",
                "identifier": "",
                "var_note": "",
            },
            {
                "table_name": "study_lookup_table",
                "var_name": "study_most_recent_appl",
                "var_label": "Most recent appl_id for the study",
                "var_fmt": "VARCHAR(8)",
                "var_length": "8",
                "identifier": "",
                "var_note": "",
            },
            {
                "table_name": "study_lookup_table",
                "var_name": "study_first_appl",
                "var_label": "First appl_id Stewards track for the study",
                "var_fmt": "VARCHAR(8)",
                "var_length": "8",
                "identifier": "",
                "var_note": "Stewards do not track any appl_ids that predate the first appl_id for the study which NIH indicated is HEAL-funded.",
            },
            {
                "table_name": "study_lookup_table",
                "var_name": "compound_key",
                "var_label": "The appl_id and hdp_id pair that uniquely identify rows, concatenated together",
                "var_fmt": "VARCHAR(17)",
                "var_length": "17",
                "identifier": "",
                "var_note": "",
            },
        ]
    )
    
    return study_table, study_table_dd


def main():
    parser = argparse.ArgumentParser(description="Build HEAL study_lookup_table from MySQL and reporter DQ audit sources.")
    parser.add_argument("--mysql-data", required=True, help="Path to mysql_<date>.dta or mysql_<date>.csv")
    parser.add_argument("--reporter-dqaudit", required=True, help="Path to reporter_dqaudit_*.csv")
    parser.add_argument("--manual-matches", help="Optional path to study_manual_matches.xlsx or CSV for step 5e")
    parser.add_argument("--manual-matches-sheet", default="matches_2026-03-23", help="Sheet name to read from --manual-matches xlsx (ignored for CSV)")
    parser.add_argument("--output-dir", default=".", help="Directory to write study_lookup_table.csv and study_table_dd.csv")
    parser.add_argument("--debug-dir", help="Optional directory to write intermediate debug CSVs")
    args = parser.parse_args()

    mysql_df = load_dataframe(Path(args.mysql_data))
    reporter_dqaudit_df = load_dataframe(Path(args.reporter_dqaudit))
    manual_matches_df = None
    if args.manual_matches:
        manual_matches_df = load_manual_matches(Path(args.manual_matches), sheet_name=args.manual_matches_sheet)

    debug_dir = Path(args.debug_dir) if args.debug_dir else None
    if debug_dir is not None:
        debug_dir.mkdir(parents=True, exist_ok=True)

    study_table, study_table_dd = build_study_lookup_table(
        mysql_df,
        reporter_dqaudit_df,
        manual_matches_df=manual_matches_df,
        export_debug=debug_dir,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    study_table.to_csv(output_dir / "study_lookup_table.csv", index=False)
    study_table_dd.to_csv(output_dir / "study_table_dd.csv", index=False)
    print(f"Wrote study_lookup_table.csv and study_table_dd.csv to {output_dir}")


if __name__ == "__main__":
    main()
