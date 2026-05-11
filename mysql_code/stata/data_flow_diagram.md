# HEAL Stata Pipeline — Data Flow Diagram

Each numbered script is run sequentially by `HEAL_00_Master.do`.
`HEAL_valuelabels.do` is called once at startup and applies value labels in all downstream scripts.

```mermaid
flowchart TD
    classDef script  fill:#4a90d9,color:#fff,stroke:#2c6db5
    classDef file    fill:#e8f5e9,color:#333,stroke:#5a9e5a
    classDef ext     fill:#fff8e1,color:#333,stroke:#c89500
    classDef mysql   fill:#e3f2fd,color:#333,stroke:#1e88e5
    classDef out     fill:#fce4e4,color:#333,stroke:#c0392b

    %% ── External / Manual Inputs ─────────────────────────────────────────────

    subgraph Ext["Manual / External Inputs"]
        direction TB
        ResNetXLS["HEAL_research_networks_ref_table_for_MySQL.xlsx\n(curator-maintained)"]:::ext
        NIH_API["NIH Reporter API\n(manual export CSV)"]:::ext
        StudyMatches["study_manual_matches.xlsx\n(curator-maintained)"]:::ext
        Monday["Monday.com DD Tracker\n(manual export)"]:::ext
        FOA_NOA["correct_foanoa_values.dta\n(NIH award emails FY24/25)"]:::ext
    end

    %% ── MySQL Raw Exports ────────────────────────────────────────────────────

    subgraph MySQLExp["MySQL Exports  (date-stamped CSVs / DTAs, $raw/)"]
        direction TB
        m_rep["reporter_$today.csv"]:::mysql
        m_awd["awards_$today.csv"]:::mysql
        m_pt["progress_tracker_$today.csv"]:::mysql
        m_rn["research_networks_$today.csv"]:::mysql
        m_pi["pi_emails_$today.csv"]:::mysql
    end

    %% ── Scripts ──────────────────────────────────────────────────────────────

    S_vl["HEAL_valuelabels.do\n(shared value labels)"]:::script

    S01["01_ResNetDocTables.do\nPrepare research-network docs for MySQL ingest"]:::script
    S02["02_ImportMerge.do\nImport MySQL CSVs, merge, clean"]:::script
    S03["03_DQAudit.do\nFind related appl_ids via NIH Reporter serial numbers"]:::script
    S04["04_StudyTable.do\nAssign xstudy_id; build study_lookup_table"]:::script
    S05["05_EngagementTable.do\nBuild per-study engagement flags"]:::script
    S06["06_CompilebyStudy.do\nRight-join all tables to study_lookup_table"]:::script
    S07["07_QC.do\nData quality report"]:::script
    S08["08_GTDTargets.do\nGet-the-Data target lists for PMs"]:::script
    S09["09_StudyMetrics.do\nHDE study-metrics report"]:::script

    %% ── Intermediate Files ───────────────────────────────────────────────────

    res_net_csv["res_net_ref_table.csv\nres_net_value_overrides_byappl.csv\n($doc/)"]:::file
    rn_mysql[/"MySQL: research_networks table\n(after manual SQL import + export)"/]:::mysql

    nihtables["nihtables_$today.dta\n(reporter ⋈ awards, $temp/)"]:::file
    mysql_dta["mysql_$today.dta\n(fully merged & cleaned, $der/)"]:::file

    dqaudit["reporter_dqaudit.dta / .csv / .xlsx\n($der/)"]:::file
    slt["study_lookup_table.dta / .csv\n($der/)"]:::file
    eng_flags["engagement_flags.dta / .csv\n($der/)"]:::file
    alldata["alldata_$today.dta / .csv\n($der/)"]:::file

    %% ── Final Output Reports ─────────────────────────────────────────────────

    subgraph Reports["Final Outputs"]
        QCDoc["QCReport_$today.doc\n($qc/)"]:::out
        GTD["gtd_targets_2026_$today.xlsx\n($out/GTD_Targets/)"]:::out
        MetricsDoc["StudyMetrics_$today.doc\n($qc/)"]:::out
    end

    %% ── Data Flow ────────────────────────────────────────────────────────────

    %% 01 → research networks
    ResNetXLS  --> S01
    S01        --> res_net_csv
    res_net_csv -->|"manual SQL:\ncreate_res_net_doc_tables\n+ update_research_networks"| rn_mysql
    rn_mysql   -->|"export → research_networks_$today.csv"| m_rn

    %% 02 → merged tables
    S_vl   -.->|"value labels (used by all)"| S02
    m_rep  --> S02
    m_awd  --> S02
    m_pt   --> S02
    m_rn   --> S02
    m_pi   --> S02
    FOA_NOA --> S02
    S02 --> nihtables
    S02 --> mysql_dta

    %% 03 → DQ audit
    nihtables  --> S03
    NIH_API    --> S03
    S03        --> dqaudit

    %% 04 → study lookup table
    mysql_dta    --> S04
    dqaudit      --> S04
    StudyMatches --> S04
    S04          --> slt

    %% 05 → engagement flags
    nihtables --> S05
    m_rn      --> S05
    slt       --> S05
    S05       --> eng_flags

    %% 06 → alldata (compiled by study)
    mysql_dta  --> S06
    dqaudit    --> S06
    slt        --> S06
    eng_flags  --> S06
    m_pi       --> S06
    m_pt       --> S06
    S06        --> alldata

    %% 07 → QC report
    m_pt      --> S07
    m_awd     --> S07
    nihtables --> S07
    mysql_dta --> S07
    slt       --> S07
    S07       --> QCDoc

    %% 08 → GTD targets
    alldata --> S08
    S08     --> GTD

    %% 09 → study metrics
    mysql_dta --> S09
    Monday    --> S09
    S09       --> MetricsDoc
```

---

## Script-by-script summary

| Script | Key Inputs | Key Outputs |
|--------|-----------|-------------|
| `HEAL_valuelabels.do` | — | Stata value-label definitions (in-memory, used by all scripts) |
| `HEAL_01_ResNetDocTables.do` | `HEAL_research_networks_ref_table_for_MySQL.xlsx` | `res_net_ref_table.csv`, `res_net_value_overrides_byappl.csv`; triggers manual MySQL SQL run |
| `HEAL_02_ImportMerge.do` | MySQL exports (reporter, awards, progress_tracker, research_networks, pi_emails), `correct_foanoa_values.dta` | `nihtables_$today.dta`, `mysql_$today.dta` |
| `HEAL_03_DQAudit.do` | `nihtables_$today.dta`, NIH Reporter API CSV (manual) | `reporter_dqaudit.dta/.csv/.xlsx` |
| `HEAL_04_StudyTable.do` | `mysql_$today.dta`, `reporter_dqaudit.dta`, `study_manual_matches.xlsx` | `study_lookup_table.dta/.csv` |
| `HEAL_05_EngagementTable.do` | `nihtables_$today.dta`, `research_networks_$today.dta`, `study_lookup_table.dta` | `engagement_flags.dta/.csv` |
| `HEAL_06_CompilebyStudy.do` | `mysql_$today.dta`, `reporter_dqaudit.dta`, `study_lookup_table.dta`, `engagement_flags.dta`, `pi_emails_$today.dta`, `progress_tracker_$today.dta` | `alldata_$today.dta/.csv` |
| `HEAL_07_QC.do` | `progress_tracker_$today.dta`, `awards_$today.dta`, `nihtables_$today.dta`, `mysql_$today.dta`, `study_lookup_table.dta` | `QCReport_$today.doc` |
| `HEAL_08_GTDTargets.do` | `alldata_$today.dta` | `gtd_targets_2026_$today.xlsx` |
| `HEAL_09_StudyMetrics.do` | `mysql_$today.dta`, Monday.com DD Tracker export (manual) | `StudyMetrics_$today.doc` |

### Archived / out-of-tree scripts

| Script | Notes |
|--------|-------|
| `HEAL_96_CTN.do` | One-time run (2024): builds CTN protocol crosswalk from CTN Excel + NIH Reporter API + `mysql_$today.dta`; outputs CTN crosswalk CSV/DTA and CTN `appl_ids` |
| `HEAL_TableArchiving.do` | Manages MySQL table archiving; not part of the regular pipeline |
| `HEAL_scratch.do` | Ad-hoc queries; not part of the regular pipeline |

### Manual steps in the pipeline

1. **Before running 01** — ensure `HEAL_research_networks_ref_table_for_MySQL.xlsx` is up to date.
2. **After 01, before 02** — run the SQL scripts `create_res_net_doc_tables` and `update_research_networks` in MySQL, then export `research_networks` as a date-stamped CSV.
3. **Before 03** — run a manual NIH Reporter API query and save the CSV export to `$raw/`.
4. **Before 09 (optional)** — export the Monday.com Data Dictionary Tracker board and manually reformat it into separate tabs (one per board section) before saving.
