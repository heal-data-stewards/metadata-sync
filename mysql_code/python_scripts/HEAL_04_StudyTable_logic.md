# HEAL_04_StudyTable — Logic Overview

This document describes the study ID assignment algorithm implemented in `HEAL_04_StudyTable.py`.

---

## High-level flow

```mermaid
flowchart TD
    IN1[MySQL data] --> PREP
    IN2[Reporter DQ Audit\nnew appl_ids not in MySQL] --> PREP

    PREP["Step 1 — Prep & Clean\nFilter empty/Other appl_ids\nConcatenate sources\n→ combined_df  2951 rows"]

    PREP --> SIS["Assign xstudy_id_stewards\ngroupby proj_ser_num + subproj_id + sfx_code\none integer ID per unique NIH project lineage"]
    SIS --> SID["Assign initial study_id\ngroupby xstudy_id_stewards + hdp_id\nonly for rows with a valid hdp_id"]
    SID --> COUNTS["Compute per-group QC counts\nnum_appl_by_xstudyidstewards\nnum_hdp_by_xstudyidstewards\nnum_live_hdps  num_arch_hdps\nnum_hdp_by_appl"]

    COUNTS --> QC{"Flag valid / invalid rows\nvalid_flag = 1 if uniquely mappable"}

    QC -->|valid_flag = 1| GOOD["mysql_hasappls_df\nrows with deterministic study_id"]
    QC -->|valid_flag = 0| BAD["studyidbad\nrows needing resolution"]

    %% ── Good-path splits ──────────────────────────────────────────
    GOOD --> G1["studyidgood1\nmany appls, each uniquely matched\nto one HDP — keep existing study_id"]
    GOOD --> G2["studyidgood2\n1 appl → multiple HDPs\nnot included in studyidkey"]
    GOOD --> G3["studyidgood3\nappls with no HDP match\n→ assign brand-new study_ids"]
    GOOD --> G4["studyidgood4\nborrow study_id from a sibling appl\nin the same stewards group"]

    %% ── studyidbad resolution ─────────────────────────────────────
    BAD --> XJOIN["Cross-join within stewards group\nbad_missing rows × bad_nonmiss rows\none row per possible appl↔HDP pairing"]

    XJOIN --> S5A["5a — Unique act_code match\ncount_actcode_matches == 1\n→ studyidgood5a  unique"]
    XJOIN --> S5B["5b — Unique appl pair\nnum_appl_pairs == 1\n→ studyidgood5b  MULTI"]
    XJOIN --> S5C["5c — Single live HDP\nnum_live_hdps == 1\n→ studyidgood5c  unique"]
    XJOIN --> S5D["5d — All HDPs archived\nnum_live_hdps == 0\nmatch by budget date proximity"]
    XJOIN --> S5E["5e — Remaining\nnum_live_hdps > 1\n+ unmatched from 5d\n→ manual review file"]

    S5D --> D1["5d_1 — single match\n→ unique"]
    S5D --> D2["5d_2 — multi-study match\n→ MULTI"]
    S5D --> NM["nomatches5d\nno date match found\n→ fed into 5e"]

    NM --> S5E

    S5E --> E1["5e_1 — curator matched, rowcount=1\n→ unique"]
    S5E --> E2["5e_2 — curator found no match\n→ brand-new study_ids"]
    S5E --> E3["5e_3 — curator matched, rowcount>1\n→ MULTI"]

    BAD --> G6["studyidgood6\nbad_nonmiss records\n199 rows with known study_id\nfrom the nonmiss side"]

    %% ── Assembly ──────────────────────────────────────────────────
    G1 & G3 & G4 --> KEY
    S5A & S5C & D1 & E1 & E2 --> KEY
    G6 --> KEY
    KEY["studyidkey\n2918 rows\ncompound_key is unique"]

    S5B & D2 & E3 --> MULTI["studyidmulti\n75 rows\n1 appl_id → multiple study_ids\nhandled separately"]

    %% ── Merge back ────────────────────────────────────────────────
    KEY --> MERGE["Merge studyidkey → full dataset\n1:1 on compound_key\n→ study_id_final assigned"]
    MULTI --> MERGE2["Merge studyidmulti → dataset\n1:m on compound_key\nfill remaining missing study_id_final"]
    MERGE --> MERGE2
    MERGE2 --> FULL["mysql_studyid\n2993 rows\n0 missing study_id_final"]

    %% ── Post-processing ───────────────────────────────────────────
    FULL --> R7["Step 7\nMost recent appl_id per study\nfilter by latest fisc_yr → bgt_end → awd_not_date"]
    FULL --> R8["Step 8\nFirst / earliest appl_id per study\nfilter by earliest fisc_yr → bgt_strt → awd_not_date"]
    FULL --> R9["Step 9\nHDP ↔ appl_id mapping table\nrows with valid hdp_id only"]

    R7 & R8 & R9 & FULL --> TABLE["Step 10 — Build study_lookup_table\nDrop CTN-only study groups\nDrop DQ-audit-only groups\nappl_id + xstudy_id is composite key\n→ 2682 rows"]

    TABLE --> OUT1["study_lookup_table.csv"]
    TABLE --> OUT2["study_table_dd.csv\ndata dictionary"]
```

---

## Key concepts

| Term | Meaning |
|------|---------|
| `xstudy_id_stewards` | Integer ID for a unique NIH project lineage (proj_ser_num + subproj_id + sfx_code). Groups all awards that belong to the same parent grant. |
| `study_id` / `xstudy_id` | The final study identifier. Prefixed with `x` to signal it is volatile and may change on a new run. |
| `compound_key` | `appl_id + "_" + hdp_id` — uniquely identifies a row in the dataset. |
| `hdp_id` | HEAL Data Platform ID. Present only for studies registered on the platform. |
| `studyidbad` | Rows where the appl↔HDP mapping is ambiguous and cannot be resolved automatically. |
| `bad_missing` | Subset of studyidbad where hdp_id is absent. |
| `bad_nonmiss` | Subset of studyidbad where hdp_id is present (renamed with `z` prefix before joining). |
| `studyidmulti` | Appl_ids that genuinely belong to more than one study — included via a separate 1:m merge rather than the 1:1 studyidkey merge. |

---

## studyidbad resolution — decision tree

```
studyidbad
│
├─ 5a  count_actcode_matches == 1 ──────────────────── unique match via act_code
│      AND act_code_match == 1
│
├─ 5b  count_actcode_matches != 1                       appl is latest award for
│      AND num_appl_pairs == 1 ─────────────────────── multiple existing studies → MULTI
│
├─ 5c  count_actcode_matches != 1
│      AND num_appl_pairs != 1
│      AND num_live_hdps == 1 ──────────────────────── unique live HDP → unique match
│      AND zarchived == "live"
│
├─ 5d  count_actcode_matches != 1
│      AND num_appl_pairs != 1
│      AND num_live_hdps == 0                           all HDPs archived
│      ├─ match by min budget date gap
│      │   ├─ rowcount == 1 ──────────────────────── unique match
│      │   ├─ rowcount > 1 AND any_match == 1 ─────── MULTI
│      │   └─ rowcount > 1 AND any_match == 0 ─────── → 5e (nomatches5d)
│      └─ no valid gap found → → 5e
│
└─ 5e  num_live_hdps > 1                               truly ambiguous
       + nomatches5d from 5d                           curated manually
       ├─ rowcount == 1 ────────────────────────────── unique match (curator decision)
       ├─ any_match == 0 ───────────────────────────── no match → brand-new study_id
       └─ rowcount > 1 AND any_match == 1 ──────────── MULTI (curator found multiple)
```
