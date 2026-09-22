
CREATE TABLE `healstudies`.`awards` (
`appl_id` VARCHAR(8) NOT NULL PRIMARY KEY,
`goal` SET('OUD', 'Pain mgt', 'Cross-Cutting Research'),
`rfa` SET('Clinical Research in Pain Management', 'Cross-Cutting Research', 'Enhanced Outcomes for Infants and Children Exposed to Opioids', 'New Strategies to Prevent and Treat Opioid Addiction', 'Novel Therapeutic Options for Opioid Use Disorder and Overdose', 'Preclinical and Translational Research in Pain Management', 'Training the Next Generation of Researchers in HEAL', 'Translation of Research to Practice for the Treatment of Opioid Addiction'),
`res_prg` VARCHAR(150),
`data_src` SET('1', '1/2', '2', '3', '3/4', '3/4/5', '4', '4/5', '5', '6', '7', '8', '9'),
`heal_funded` TINYINT(1) default NULL,
`nih_aian` TINYINT(1),
`nih_core_cde` TINYINT(1),
`nih_foa_heal_lang` TINYINT(1) default NULL
);

LOAD DATA LOCAL
INFILE 'C:/Users/smccutchan/OneDrive - Research Triangle Institute/Documents/HEAL/MySQL/Raw/Derived/awards_fy24.csv'
INTO TABLE `healstudies`.`awards`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;