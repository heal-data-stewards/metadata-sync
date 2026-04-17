
/* Define the table */
/*
CREATE TABLE `healstudies`.`pi_emails` (
`appl_id` VARCHAR(8) NOT NULL,
`pi_email` VARCHAR(119)
);
*/

/* Empty the table contents */
TRUNCATE TABLE `pi_emails`;


/* Load a local data file into the table */
LOAD DATA LOCAL
INFILE 'C:/Users/smccutchan/OneDrive - Research Triangle Institute/Documents/HEAL/MySQL/NIH_Data/Derived/pi_emails_fy25.csv'
INTO TABLE `healstudies`.`pi_emails`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;