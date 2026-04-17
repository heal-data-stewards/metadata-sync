
/* Define the table */
/*
CREATE TABLE `healstudies`.`reporter_dqaudit` (
`proj_abs` TEXT,
`act_code` VARCHAR(10),
`ic_code` VARCHAR(10),
`adm_ic` VARCHAR(25),
`adm_ic_code` VARCHAR(10),
`adm_ic_nm` VARCHAR(250),
`fund_ic` VARCHAR(100),
`ic_fund_code` VARCHAR(10),
`ic_fund_yr` VARCHAR(50),
`fund_ic_nm` VARCHAR(400),
`fund_ic_tot_cst` VARCHAR(100),
`appl_id` VARCHAR(15) PRIMARY KEY,
`arra_fund` VARCHAR(10),
`tot_fund` INT,
`awd_not_date` TEXT,
`awd_ty` VARCHAR(10),
`bgt_end` TEXT,
`bgt_strt` TEXT,
`cfda_code` VARCHAR(10),
`cong_dist` VARCHAR(15),
`ctc_pi_nm` VARCHAR(150),
`cr_pro_num` VARCHAR(255),
`covid_res` VARCHAR(15),
`amt_dir` INT,
`fisc_yr` INT,
`ful_foa` VARCHAR(25),
`sty_sec_ful_grp_code` VARCHAR(10),
`sty_sec_ful_nm` VARCHAR(200),
`sty_sec_ful_des_code` VARCHAR(15),
`sty_sec_ful_flex_code` VARCHAR(5),
`sty_sec_ful_srg_code` VARCHAR(10),
`sty_sec_ful_srg_flex` VARCHAR(5),
`fund_mech` VARCHAR(50),
`indct_cst_amt` INT,
`is_act` TEXT,
`is_new` TEXT,
`mech_code_dc` VARCHAR(10),
`org_dept_type` VARCHAR(50),
`org_ext_id` VARCHAR(15),
`org_cy` VARCHAR(50),
`org_ctry` VARCHAR(25),
`org_duns` VARCHAR(50),
`org_fips` VARCHAR(5),
`org_ipf_code` VARCHAR(15),
`org_nm` VARCHAR(150),
`org_st` VARCHAR(2),
`org_zip_code` VARCHAR(15),
`org_ty_code` VARCHAR(5),
`org_ty_oth` TEXT,
`org_ty_nm` VARCHAR(100),
`phr_text` TEXT,
`pref_terms` TEXT,
`pi_fst_nm` VARCHAR(100),
`pi` VARCHAR(300),
`pi_is_ctc` VARCHAR(75),
`pi_lst_nm` VARCHAR(200),
`pi_mid_nm` VARCHAR(50),
`pi_prof_id` VARCHAR(150),
`pi_title` VARCHAR(150),
`prg_ofc_fst_nm` VARCHAR(50),
`prg_ofc` VARCHAR(50),
`prg_ofc_lst_nm` VARCHAR(50),
`prg_ofc_mid_nm` VARCHAR(50),
`proj_url` VARCHAR(150),
`proj_end_date` DATE,
`proj_num` VARCHAR(50),
`proj_num_spl_act_code` VARCHAR(5),
`proj_num_spl_ty_code` VARCHAR(5),
`proj_nm_spl_supp_yr` VARCHAR(10),
`proj_num_spl_ic_code` VARCHAR(5),
`proj_ser_nm_spl` VARCHAR(10),
`proj_num_spl_sfx_code` VARCHAR(10),
`proj_nm_spl_yr` VARCHAR(5),
`proj_ser_num` VARCHAR(25),
`proj_strt_date` DATE,
`proj_title` VARCHAR(400),
`spd_cat` VARCHAR(200),
`spd_cat_0` TEXT,
`subproj_id` VARCHAR(10),
`trms` TEXT
);
*/
CHECK TABLE `healstudies`.`reporter_dqaudit`;


TRUNCATE TABLE `healstudies`.`reporter_dqaudit`;


/* Load a local data file into the table */
LOAD DATA LOCAL
INFILE 'C:/Users/smccutchan/Documents/HEAL/MySQL/Derived/reporter_dqaudit.csv'
INTO TABLE `healstudies`.`reporter_dqaudit`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

