/* Define the table */
CREATE TABLE `healstudies`.`research_networks` (
`appl_id` VARCHAR(8) NOT NULL,
`res_net` VARCHAR(19),
`res_net_override_flag` BOOLEAN);

/* Load a local data file into the table */
LOAD DATA LOCAL
INFILE 'C:/Users/smccutchan/OneDrive - Research Triangle Institute/Documents/HEAL/MySQL/Derived/research_networks.csv'
INTO TABLE research_networks
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;