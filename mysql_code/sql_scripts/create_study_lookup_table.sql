/* Define the table */
/*
CREATE TABLE `healstudies`.`study_lookup_table` (
`appl_id` VARCHAR(8) NOT NULL,
`xstudy_id` VARCHAR(4) NOT NULL,
`study_hdp_id` CHAR(8),
`study_hdp_id_appl` VARCHAR(8),
`study_most_recent_appl` VARCHAR(8),
`study_first_appl` VARCHAR(8),
`compound_key` VARCHAR(17)
);
*/
/*FOREIGN KEY (study_hdp_id)  REFERENCES progress_tracker(hdp_id)*/

USE healstudies;

/* Empty the table's contents */
TRUNCATE TABLE `study_lookup_table`;

/* Load a local data file into the table */
LOAD DATA LOCAL
INFILE 'C:/Users/smccutchan/OneDrive - Research Triangle Institute/Documents/HEAL/MySQL/Derived/study_lookup_table.csv'
INTO TABLE study_lookup_table
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;