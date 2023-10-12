# Databricks notebook source
# MAGIC %pip install git+https://github.com/tobymao/sqlglot.git

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- summarize results
# MAGIC SELECT count(1) count, statement_type, strategy, error_class, cast(insert_dt as DATE)
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC GROUP BY 2,3,4,5
# MAGIC ORDER BY 1 DESC, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from douglas_moore.sqlglot.project1
# MAGIC where statement_type = 'UPDATE FROM'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC where statement_type = "CREATE TABLE"
# MAGIC and error_class is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog douglas_moore;
# MAGIC CREATE TABLE `dbo`.`t_cmm_dim_geo` (
# MAGIC   `zip_cd_mkey` INT NOT NULL, `zip_cd` VARCHAR(5), 
# MAGIC   `zip_tp_cd` CHAR(1), `zip_tp_nm_tx` VARCHAR(18), 
# MAGIC   `city_tp_cd` CHAR(1), `city_tp_nm_tx` VARCHAR(26), `city_nm_tx` VARCHAR(64), `state_fips_cd` VARCHAR(2), `state_cd` VARCHAR(2), `state_nm_tx` VARCHAR(64), `msa_cd` VARCHAR(4), `msa_tp_cd` VARCHAR(5), `msa_nm_tx` VARCHAR(64), 
# MAGIC   `cbsa_cd` VARCHAR(5), `cbsa_tp_cd` VARCHAR(3), `cbsa_tp_nm` VARCHAR(12), `cbsa_nm_tx` VARCHAR(64), `csa_cd` VARCHAR(3), `csa_nm_tx` VARCHAR(64), `dvsn_cd` VARCHAR(5), `dvsn_nm_tx` VARCHAR(64), `cnty_fips_cd` VARCHAR(5), `cnty_nm_tx` VARCHAR(64), `area_cd` VARCHAR(16), `tm_zn_cd` VARCHAR(16), `utc_cd` INT, `dst_ind` CHAR(1), `lat_num` VARCHAR(11), `lon_num` VARCHAR(11), `version` INT, `date_from` TIMESTAMP, `date_to` TIMESTAMP,
# MAGIC   `safe_harbor_zip_cd` STRING GENERATED ALWAYS AS (
# MAGIC     (CASE WHEN LEFT(`zip_cd`, (3)) = '893' OR LEFT(`zip_cd`, (3)) = '890' OR LEFT(`zip_cd`, (3)) = '884' OR LEFT(`zip_cd`, (3)) = '879' OR LEFT(`zip_cd`, (3)) = '878' OR LEFT(`zip_cd`, (3)) = '831' OR LEFT(`zip_cd`, (3)) = '830' OR LEFT(`zip_cd`, (3)) = '823' OR LEFT(`zip_cd`, (3)) = '821' OR LEFT(`zip_cd`, (3)) = '790' OR LEFT(`zip_cd`, (3)) = '692' OR LEFT(`zip_cd`, (3)) = '556' OR LEFT(`zip_cd`, (3)) = '203' OR LEFT(`zip_cd`, (3)) = '102' OR LEFT(`zip_cd`, (3)) = '063' OR LEFT(`zip_cd`, (3)) = '059' OR LEFT(`zip_cd`, (3)) = '036' THEN LEFT(`zip_cd`, (3)) + 'XX' ELSE `zip_cd` END)
# MAGIC     ), CONSTRAINT `pk_t_cmm_dim_geo__zip_cd_mkey` PRIMARY KEY (`zip_cd_mkey`)
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS zipcodes;
# MAGIC EXPLAIN COST CREATE TEMPORARY TABLE zipcodes (zipcode VARCHAR(5), State VARCHAR(2)) USING PARQUET;

# COMMAND ----------

CREATE TABLE [dbo].[t_cmm_dim_geo](
	[zip_cd_mkey] [int] NOT NULL,
	[zip_cd] [varchar](5) NULL,
	[date_to] [datetime] NULL,
	[safe_harbor_zip_cd]  AS (case when left([zip_cd],(3))='893' OR left([zip_cd],(3))='890' OR left([zip_cd],(3))='884' OR left([zip_cd],(3))='879' OR left([zip_cd],(3))='878' OR left([zip_cd],(3))='831' OR left([zip_cd],(3))='830' OR left([zip_cd],(3))='823' OR left([zip_cd],(3))='821' OR left([zip_cd],(3))='790' OR left([zip_cd],(3))='692' OR left([zip_cd],(3))='556' OR left([zip_cd],(3))='203' OR left([zip_cd],(3))='102' OR left([zip_cd],(3))='063' OR left([zip_cd],(3))='059' OR left([zip_cd],(3))='036' then left([zip_cd],(3))+'XX' else [zip_cd] end) PERSISTED,
 CONSTRAINT [pk_t_cmm_dim_geo__zip_cd_mkey] PRIMARY KEY CLUSTERED 
(
	[zip_cd_mkey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `dbo`.`t_cmm_dim_geo` (`zip_cd_mkey` INT NOT NULL, `zip_cd` VARCHAR(5), 
# MAGIC `date_to` TIMESTAMP, 
# MAGIC `safe_harbor_zip_cd` AS (
# MAGIC   CASE WHEN LEFT(`zip_cd`, (3)) = '893' OR LEFT(`zip_cd`, (3)) = '890' OR LEFT(`zip_cd`, (3)) = '884' OR LEFT(`zip_cd`, (3)) = '879' OR LEFT(`zip_cd`, (3)) = '878' OR LEFT(`zip_cd`, (3)) = '831' OR LEFT(`zip_cd`, (3)) = '830' OR LEFT(`zip_cd`, (3)) = '823' OR LEFT(`zip_cd`, (3)) = '821' OR LEFT(`zip_cd`, (3)) = '790' OR LEFT(`zip_cd`, (3)) = '692' OR LEFT(`zip_cd`, (3)) = '556' OR LEFT(`zip_cd`, (3)) = '203' OR LEFT(`zip_cd`, (3)) = '102' OR LEFT(`zip_cd`, (3)) = '063' OR LEFT(`zip_cd`, (3)) = '059' OR LEFT(`zip_cd`, (3)) = '036' THEN LEFT(`zip_cd`, (3)) + 'XX' ELSE `zip_cd` END) PERSISTED,
# MAGIC CONSTRAINT `pk_t_cmm_dim_geo__zip_cd_mkey` PRIMARY KEY (`zip_cd_mkey`)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE x (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 3 INCREMENT BY 7))

# COMMAND ----------

ParseException("\n[PARSE_SYNTAX_ERROR] Syntax error at or near 'AS'.(line 1, pos 62)\n\n== SQL ==\nexplain COST CREATE TABLE `dbo`.`T_Users` (`id` INT GENERATED AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, `username` VARCHAR(64) NOT NULL, `password` VARCHAR(64) NOT NULL, `legacy_pw` VARCHAR(64), `display_name` VARCHAR(255), `organization_name` VARCHAR(255), `email` VARCHAR(255), `is_prescriber` INT, `prescriber_street` VARCHAR(255), `prescriber_city` VARCHAR(255), `prescriber_state` VARCHAR(2), `prescriber_zip` VARCHAR(16), `office_phone` VARCHAR(32), `office_fax` VARCHAR(32), `office_contact` VARCHAR(255), `npi` VARCHAR(255), `created_on` TIMESTAMP, `is_deleted` BOOLEAN, `druglist` VARCHAR(8000), `deleted` BOOLEAN NOT NULL, `deleted_on` TIMESTAMP, `role` SMALLINT, `eula_agree` BOOLEAN NOT NULL, `notify_me` TINYINT, `last_notification_dismissed` TIMESTAMP NOT NULL, `last_password_attempt` TIMESTAMP, `password_attempt_count` INT, `user_type` INT, `referral_source` VARCHAR(500), `customer_status` VARCHAR(50), `show_history` TINYINT NOT NULL, `user_agent` VARCHAR(500), `requests_per_week` VARCHAR(50), `reminder_email` INT NOT NULL, `form_search` BOOLEAN NOT NULL, `ncpdp_id` VARCHAR(7), `autofill_address` BOOLEAN NOT NULL, `cmm_usage` VARCHAR(500), `org_description` VARCHAR(500), `prescriber_specialty` VARCHAR(50), `rep_territory` VARCHAR(50), `signup_complete` BOOLEAN NOT NULL, `number_beds` VARCHAR(50), `referral_type` VARCHAR(25), `pa_lead` BOOLEAN NOT NULL, `ba_agree` BOOLEAN NOT NULL, `no_offers` BOOLEAN NOT NULL, `olark_chat` BOOLEAN NOT NULL, `software_vendor` VARCHAR(50), `auto_notify` TINYINT, `userbranding_id` INT, `coversheet_only` TINYINT, `web_iprange` VARCHAR(255), `claims_iprange` VARCHAR(255), `power_dashboard` BOOLEAN NOT NULL, `phone_to_fax` BOOLEAN NOT NULL, `renotify_on_dup` BOOLEAN NOT NULL, `defer_form_choice` BOOLEAN NOT NULL, `account_id` INT, `renotify_interval` INT, `reminder_interval` INT, `faxaction_interval` INT, `is_confirmed` BOOLEAN, `confirmation_token` VARCHAR(32), `confirmation_create_date` T...")

# COMMAND ----------

import sqlglot

# COMMAND ----------

sql = '''CREATE TABLE [dbo].[T_Users](
	[id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[username] [varchar](64) NOT NULL,
	[password] [varchar](64) NOT NULL,
	[legacy_pw] [varchar](64) NULL,
	[display_name] [varchar](255) NULL,
	[organization_name] [varchar](255) NULL,
	[email] [varchar](255) NULL,
	[is_prescriber] [int] NULL,
	[prescriber_street] [varchar](255) NULL,
	[prescriber_city] [varchar](255) NULL,
	[prescriber_state] [varchar](2) NULL,
	[prescriber_zip] [varchar](16) NULL,
	[office_phone] [varchar](32) NULL,
	[office_fax] [varchar](32) NULL,
	[office_contact] [varchar](255) NULL,
	[npi] [varchar](255) NULL,
	[created_on] [datetime] NULL,
	[is_deleted] [bit] NULL,
	[druglist] [varchar](8000) NULL,
	[deleted] [bit] NOT NULL,
	[deleted_on] [datetime] NULL,
	[role] [smallint] NULL,
	[eula_agree] [bit] NOT NULL,
	[notify_me] [tinyint] NULL,
	[last_notification_dismissed] [datetime] NOT NULL,
	[last_password_attempt] [datetime] NULL,
	[password_attempt_count] [int] NULL,
	[user_type] [int] NULL,
	[referral_source] [varchar](500) NULL,
	[customer_status] [varchar](50) NULL,
	[show_history] [tinyint] NOT NULL,
	[user_agent] [varchar](500) NULL,
	[requests_per_week] [varchar](50) NULL,
	[reminder_email] [int] NOT NULL,
	[form_search] [bit] NOT NULL,
	[ncpdp_id] [varchar](7) NULL,
	[autofill_address] [bit] NOT NULL,
	[cmm_usage] [varchar](500) NULL,
	[org_description] [varchar](500) NULL,
	[prescriber_specialty] [varchar](50) NULL,
	[rep_territory] [varchar](50) NULL,
	[signup_complete] [bit] NOT NULL,
	[number_beds] [varchar](50) NULL,
	[referral_type] [varchar](25) NULL,
	[pa_lead] [bit] NOT NULL,
	[ba_agree] [bit] NOT NULL,
	[no_offers] [bit] NOT NULL,
	[olark_chat] [bit] NOT NULL,
	[software_vendor] [varchar](50) NULL,
	[auto_notify] [tinyint] NULL,
	[userbranding_id] [int] NULL,
	[coversheet_only] [tinyint] NULL,
	[web_iprange] [varchar](255) NULL,
	[claims_iprange] [varchar](255) NULL,
	[power_dashboard] [bit] NOT NULL,
	[phone_to_fax] [bit] NOT NULL,
	[renotify_on_dup] [bit] NOT NULL,
	[defer_form_choice] [bit] NOT NULL,
	[account_id] [int] NULL,
	[renotify_interval] [int] NULL,
	[reminder_interval] [int] NULL,
	[faxaction_interval] [int] NULL,
	[is_confirmed] [bit] NULL,
	[confirmation_token] [varchar](32) NULL,
	[confirmation_create_date] [datetime] NULL,
	[fax_notification] [varchar](50) NULL,
	[fax_in_intermediary] [bit] NOT NULL,
	[locked] [bit] NOT NULL,
	[job_title] [varchar](50) NULL,
	[password_reset_token] [varchar](36) NULL,
	[password_reset_expires_on] [datetime] NULL,
	[bcrypt_password] [varchar](64) NULL,
 CONSTRAINT [PK_T_Users] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]'''

# COMMAND ----------

r = sqlglot.transpile(sql=sql, read='tsql',write='databricks')

# COMMAND ----------

r[0]

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC CREATE TABLE `dbo`.`T_Users` (`id` BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, `username` VARCHAR(64) NOT NULL, `password` VARCHAR(64) NOT NULL, `legacy_pw` VARCHAR(64), `display_name` VARCHAR(255), `organization_name` VARCHAR(255), `email` VARCHAR(255), `is_prescriber` INT, `prescriber_street` VARCHAR(255), `prescriber_city` VARCHAR(255), `prescriber_state` VARCHAR(2), `prescriber_zip` VARCHAR(16), `office_phone` VARCHAR(32), `office_fax` VARCHAR(32), `office_contact` VARCHAR(255), `npi` VARCHAR(255), `created_on` TIMESTAMP, `is_deleted` BOOLEAN, `druglist` VARCHAR(8000), `deleted` BOOLEAN NOT NULL, `deleted_on` TIMESTAMP, `role` SMALLINT, `eula_agree` BOOLEAN NOT NULL, `notify_me` TINYINT, `last_notification_dismissed` TIMESTAMP NOT NULL, `last_password_attempt` TIMESTAMP, `password_attempt_count` INT, `user_type` INT, `referral_source` VARCHAR(500), `customer_status` VARCHAR(50), `show_history` TINYINT NOT NULL, `user_agent` VARCHAR(500), `requests_per_week` VARCHAR(50), `reminder_email` INT NOT NULL, `form_search` BOOLEAN NOT NULL, `ncpdp_id` VARCHAR(7), `autofill_address` BOOLEAN NOT NULL, `cmm_usage` VARCHAR(500), `org_description` VARCHAR(500), `prescriber_specialty` VARCHAR(50), `rep_territory` VARCHAR(50), `signup_complete` BOOLEAN NOT NULL, `number_beds` VARCHAR(50), `referral_type` VARCHAR(25), `pa_lead` BOOLEAN NOT NULL, `ba_agree` BOOLEAN NOT NULL, `no_offers` BOOLEAN NOT NULL, `olark_chat` BOOLEAN NOT NULL, `software_vendor` VARCHAR(50), `auto_notify` TINYINT, `userbranding_id` INT, `coversheet_only` TINYINT, `web_iprange` VARCHAR(255), `claims_iprange` VARCHAR(255), `power_dashboard` BOOLEAN NOT NULL, `phone_to_fax` BOOLEAN NOT NULL, `renotify_on_dup` BOOLEAN NOT NULL, `defer_form_choice` BOOLEAN NOT NULL, `account_id` INT, `renotify_interval` INT, `reminder_interval` INT, `faxaction_interval` INT, `is_confirmed` BOOLEAN, `confirmation_token` VARCHAR(32), `confirmation_create_date` TIMESTAMP, `fax_notification` VARCHAR(50), `fax_in_intermediary` BOOLEAN NOT NULL, `locked` BOOLEAN NOT NULL, `job_title` VARCHAR(50), `password_reset_token` VARCHAR(36), `password_reset_expires_on` TIMESTAMP, `bcrypt_password` VARCHAR(64), CONSTRAINT `PK_T_Users` PRIMARY KEY (`id`))

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC CREATE TABLE `dbo`.`T_Users` (`id` BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, `username` VARCHAR(64) NOT NULL, `password` VARCHAR(64) NOT NULL, `legacy_pw` VARCHAR(64), `display_name` VARCHAR(255), `organization_name` VARCHAR(255), `email` VARCHAR(255), `is_prescriber` INT, `prescriber_street` VARCHAR(255), `prescriber_city` VARCHAR(255), `prescriber_state` VARCHAR(2), `prescriber_zip` VARCHAR(16), `office_phone` VARCHAR(32), `office_fax` VARCHAR(32), `office_contact` VARCHAR(255), `npi` VARCHAR(255), `created_on` TIMESTAMP, `is_deleted` BOOLEAN, `druglist` VARCHAR(8000), `deleted` BOOLEAN NOT NULL, `deleted_on` TIMESTAMP, `role` SMALLINT, `eula_agree` BOOLEAN NOT NULL, `notify_me` TINYINT, `last_notification_dismissed` TIMESTAMP NOT NULL, `last_password_attempt` TIMESTAMP, `password_attempt_count` INT, `user_type` INT, `referral_source` VARCHAR(500), `customer_status` VARCHAR(50), `show_history` TINYINT NOT NULL, `user_agent` VARCHAR(500), `requests_per_week` VARCHAR(50), `reminder_email` INT NOT NULL, `form_search` BOOLEAN NOT NULL, `ncpdp_id` VARCHAR(7), `autofill_address` BOOLEAN NOT NULL, `cmm_usage` VARCHAR(500), `org_description` VARCHAR(500), `prescriber_specialty` VARCHAR(50), `rep_territory` VARCHAR(50), `signup_complete` BOOLEAN NOT NULL, `number_beds` VARCHAR(50), `referral_type` VARCHAR(25), `pa_lead` BOOLEAN NOT NULL, `ba_agree` BOOLEAN NOT NULL, `no_offers` BOOLEAN NOT NULL, `olark_chat` BOOLEAN NOT NULL, `software_vendor` VARCHAR(50), `auto_notify` TINYINT, `userbranding_id` INT, `coversheet_only` TINYINT, `web_iprange` VARCHAR(255), `claims_iprange` VARCHAR(255), `power_dashboard` BOOLEAN NOT NULL, `phone_to_fax` BOOLEAN NOT NULL, `renotify_on_dup` BOOLEAN NOT NULL, `defer_form_choice` BOOLEAN NOT NULL, `account_id` INT, `renotify_interval` INT, `reminder_interval` INT, `faxaction_interval` INT, `is_confirmed` BOOLEAN, `confirmation_token` VARCHAR(32), `confirmation_create_date` TIMESTAMP, `fax_notification` VARCHAR(50), `fax_in_intermediary` BOOLEAN NOT NULL, `locked` BOOLEAN NOT NULL, `job_title` VARCHAR(50), `password_reset_token` VARCHAR(36), `password_reset_expires_on` TIMESTAMP, `bcrypt_password` VARCHAR(64), CONSTRAINT `PK_T_Users` PRIMARY KEY (`id`))

# COMMAND ----------

# MAGIC %md ## Create table

# COMMAND ----------

import sqlglot


# COMMAND ----------

tsql = """CREATE TABLE dbo.Products
    (
       ProductID int IDENTITY (1,1) NOT NULL
       , QtyAvailable smallint
       , SaleDate date
       , UnitPrice money
       , InventoryValue AS QtyAvailable * UnitPrice
       , yr AS YEAR(SaleDate)
     );"""

# COMMAND ----------

dbx_sql = sqlglot.transpile(tsql, read='tsql',write='databricks')[0]
dbx_sql

# COMMAND ----------

CREATE TABLE dbo.Products (
    ProductID INT GENERATED AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, 
    QtyAvailable SMALLINT, 
    SaleDate DATE, 
    UnitPrice DECIMAL(15, 4), 
    InventoryValue AS QtyAvailable * UnitPrice, 
    yr AS YEAR(SaleDate)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog douglas_moore;
# MAGIC
# MAGIC CREATE TABLE dbo.Products (
# MAGIC   ProductID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, 
# MAGIC   QtyAvailable SMALLINT, 
# MAGIC   SaleDate DATE, 
# MAGIC   UnitPrice DECIMAL(15, 4), 
# MAGIC   InventoryValue DECIMAL(15, 4) GENERATED ALWAYS AS (CAST(QtyAvailable * UnitPrice AS DECIMAL(15,4))) , 
# MAGIC   yr INT GENERATED ALWAYS AS (YEAR(SaleDate))
# MAGIC %  )

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog douglas_moore;
# MAGIC CREATE OR REPLACE TABLE dbo.tbl (id BIGINT NOT NULL GENERATED always AS IDENTITY (START WITH 10 INCREMENT BY 1) PRIMARY KEY)
# MAGIC

# COMMAND ----------

# MAGIC %sql describe extended dbo.tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table dbo.tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- tsql
# MAGIC UPDATE ru_new SET state = zc.state
# MAGIC     FROM t_reporting_user_merge ru_new
# MAGIC     JOIN zipcodes zc
# MAGIC     ON LEFT(ru_new.zip,5) = zc.zipcode

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog douglas_moore;
# MAGIC USE SCHEMA dbo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  zipcodes(state char(2), zipcode char(5));
# MAGIC CREATE TABLE IF NOT EXISTS tmerge(id INT, state char(2), zip char(9));

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tmerge VALUES   (1, null, '01890001');
# MAGIC INSERT INTO tmerge VALUES   (2, null, '01890002');
# MAGIC INSERT INTO tmerge VALUES   (3, null, '01890003');

# COMMAND ----------

# MAGIC %sql select * from tmerge;

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC UPDATE tmerge
# MAGIC SET state = (SELECT state
# MAGIC              FROM zipcodes
# MAGIC              WHERE LEFT(tmerge.zip, 5) = zipcodes.zipcode)

# COMMAND ----------

# MAGIC %md ## UPDATE FROM
# MAGIC ```sql
# MAGIC -- tsql
# MAGIC UPDATE ru_new SET state = zc.state
# MAGIC     FROM t_reporting_user_merge ru_new
# MAGIC     JOIN zipcodes zc
# MAGIC     ON LEFT(ru_new.zip,5) = zc.zipcode
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC --- spark sql
# MAGIC MERGE INTO tmerge
# MAGIC USING zipcodes
# MAGIC ON LEFT(tmerge.zip, 5) = zipcodes.zipcode
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET tmerge.state = zipcodes.state
# MAGIC ```

# COMMAND ----------

import sqlglot

# COMMAND ----------

sqlglot.parse_one('''UPDATE tmerge SET state = zc.state
    FROM tmerge
    JOIN zipcodes zc
    ON LEFT(tmerge.zip,5) = zc.zipcode''', read='tsql')

# COMMAND ----------

sqlglot.parse_one('''MERGE INTO tmerge
USING zipcodes
ON LEFT(tmerge.zip, 5) = zipcodes.zipcode
WHEN MATCHED THEN
    UPDATE SET tmerge.state = zipcodes.state''', read='spark')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO tmerge
# MAGIC USING zipcodes
# MAGIC ON LEFT(tmerge.zip, 5) = zipcodes.zipcode
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET tmerge.state = zipcodes.state

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmerge 

# COMMAND ----------

# MAGIC %md ## Unique constraint

# COMMAND ----------

import sqlglot

# COMMAND ----------

tsql = """CREATE TABLE [dbo].[t_external_cmm_email](
	[id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[email] [varchar](255) NOT NULL,
	[date_added] [datetime] NULL,
 CONSTRAINT [PK_t_external_cmm_email] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UN_t_external_cmm_email] UNIQUE NONCLUSTERED 
(
	[email] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]"""
sqls = sqlglot.transpile(tsql,read='tsql',write='databricks')
sqls[0]

# COMMAND ----------

sqlglot.parse_one(tsql, read='tsql')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG douglas_moore;
# MAGIC CREATE TABLE `dbo`.`t_external_cmm_email` (
# MAGIC   `email` VARCHAR(255) NOT NULL, `date_added` TIMESTAMP,
# MAGIC   ,CONSTRAINT `UN_t_external_cmm_email` UNIQUE (`email`)
# MAGIC )

# COMMAND ----------


