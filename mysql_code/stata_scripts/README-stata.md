Most Stata code is organized into a program tree, where each program is meant to be run sequentially, beginning with HEAL_00_Master.do. There are a few Stata programs which are not part of the program tree. Programs in the tree include numbers in the file name, while those outside the tree do not include numbers in the file name.

Summary of what each Stata .do file in the program tree does:
- HEAL_00_Master : This is the master Stata program for MySQL data processing. It sets global macros before calling the other programs in the tree.
- HEAL_01_ResNetDocTables : This program prepares the latest research networks documentation table for MySQL ingest.
- HEAL_02_ImportMerge : This program imports the latest data from MySQL, merges it, and cleans it.
- HEAL_03_DQAudit : This program searches NIH Reporter for appl_ids that may be related to reporter or awards table appl_ids by serial number.
- HEAL_04_StudyTable : This program creates the xstudy_id field and the study_lookup_table.
- HEAL_05_EngagementTable : This program produces the engagement_flags table.
- HEAL_06_CompilebyStudy : This program right-joins MySQL tables like reporter and research_networks to the study_lookup_table.
- HEAL_07_QC : This program creates a data quality control (QC) report.
- HEAL_08_GTDTargets : This program creates Get the Data target lists for PM colleagues.
- HEAL_09_StudyMetrics : This program produces a report of HDE study metrics.
- HEAL_96_CTN : This program generates a crosswalk for Clinical Trials Network protocols and associated project numbers. It identifies application IDs that belong to project numbers identified by responsible SMEs as part of the Clinical Trials Network (CTN). It was run once in 2024.
  
- HEAL_valuelabels : This program applies Stata value labels to variables.

Other Stata programs
- HEAL_scratch.do : This program performs ad-hoc queries.
- HEAL_TableArchiving : This program manages MySQL tables prior to archiving.
- xHEAL_02_ResNetTable: This program used to create the research_networks table for upload to MySQL. It has been replaced by a MySQL script.
