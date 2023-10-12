# Databricks notebook source
# MAGIC %md # SQLGlot Demo

# COMMAND ----------

# MAGIC %md ## Install latest branch
# MAGIC  (updated daily)

# COMMAND ----------

# MAGIC %pip install --quiet git+https://github.com/tobymao/sqlglot.git

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sqlglot

# COMMAND ----------

# MAGIC %md ## One offs

# COMMAND ----------

sql = """
SELECT a,b FROM table1 t1
"""
sqlglot.parse_one(sql=sql)

# COMMAND ----------

sql = """
SELECT a,b FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id
"""
sqlglot.parse_one(sql=sql)

# COMMAND ----------

sql = """
SELECT a,b FROM cat1.db1.table1 t1 
JOIN cat2.db2.table2 t2 ON t1.id = t2.id
"""
sqlglot.parse_one(sql=sql)

# COMMAND ----------

sql = """

CREATE TABLE [dbo].[table1] 
(
    id INT,
    a  CHAR(10),
    b TIME(6)
)
;

CREATE TABLE [dbo].[table2] 
(
    id INT,
    c  FLOAT(24),
    d  FLOAT(64)
)
;

SELECT a,b FROM [douglas_moore].[dbo].[table1] t1 JOIN douglas_moore.dbo.table2 t2 ON t1.id = t2.id
;
"""
sqlglot.parse(sql=sql, dialect="tsql")

# COMMAND ----------

# MAGIC %md ## Transpile
# MAGIC From tsql to spark(3)

# COMMAND ----------

sql = """
CREATE OR REPLACE TABLE [dbo].[table1] 
(
    id INTEGER,
    a  CHAR(10),
    b TIME(6)
)
"""
sqls = sqlglot.transpile(sql=sql, read="tsql", write="spark")
sqls

# COMMAND ----------

# MAGIC %sql USE CATALOG douglas_moore;

# COMMAND ----------

# create the transpiled statement in databricks
spark.sql(sqls[0])

# COMMAND ----------

# MAGIC %sql describe dbo.table1

# COMMAND ----------

sql = """select a,b from [dbo].[table1]"""
sqls = sqlglot.transpile(sql=sql, read="tsql", write="spark")
sqls

# COMMAND ----------

spark.sql(sqls[0])

# COMMAND ----------

# MAGIC %md ### TSQL Create Table
# MAGIC A more complex example

# COMMAND ----------

sql = """
CREATE TABLE [dbo].[t_cmm_dim_geo](
	[zip_cd_mkey] [int] NOT NULL,
	[zip_cd] [varchar](5) NULL,
	[zip_tp_cd] [char](1) NULL,
	[zip_tp_nm_tx] [varchar](18) NULL,
	[city_tp_cd] [char](1) NULL,
	[city_tp_nm_tx] [varchar](26) NULL,
	[city_nm_tx] [varchar](64) NULL,
	[state_fips_cd] [varchar](2) NULL,
	[state_cd] [varchar](2) NULL,
	[version] [int] NULL,
	[date_from] [datetime] NULL,
	[date_to] [datetime] NULL,
	[safe_harbor_zip_cd]  AS (case when left([zip_cd],(3))='893' OR left([zip_cd],(3))='890' OR left([zip_cd],(3))='884' OR left([zip_cd],(3))='879' OR left([zip_cd],(3))='878' OR left([zip_cd],(3))='831' OR left([zip_cd],(3))='830' OR left([zip_cd],(3))='823' OR left([zip_cd],(3))='821' OR left([zip_cd],(3))='790' OR left([zip_cd],(3))='692' OR left([zip_cd],(3))='556' OR left([zip_cd],(3))='203' OR left([zip_cd],(3))='102' OR left([zip_cd],(3))='063' OR left([zip_cd],(3))='059' OR left([zip_cd],(3))='036' then left([zip_cd],(3))+'XX' else [zip_cd] end) PERSISTED,
 CONSTRAINT [pk_t_cmm_dim_geo__zip_cd_mkey] PRIMARY KEY CLUSTERED 
(
	[zip_cd_mkey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
"""
sqls = sqlglot.transpile(sql=sql, read="tsql", write="databricks")
sqls[0]

# COMMAND ----------

spark.sql("EXPLAIN COST " + sqls[0])

# COMMAND ----------

# MAGIC %md # Evaluate bulk conversion results
# MAGIC switch to vscode 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- summarize results
# MAGIC SELECT count(1) count, statement_type, strategy, error_class, cast(insert_dt as DATE)
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC GROUP BY 2,3,4,5
# MAGIC ORDER BY 1 DESC, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- errors organized by file
# MAGIC SELECT count(1) cnt, file_path, error_class
# MAGIC FROM douglas_moore.sqlglot.project1 a
# MAGIC GROUP BY file_path, error_class
# MAGIC ORDER BY cnt DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 100% successful transpilations
# MAGIC select * from douglas_moore.sqlglot.project1
# MAGIC where error_class is null

# COMMAND ----------

# MAGIC %sql
# MAGIC -- total statements found
# MAGIC SELECT count(DISTINCT file_path) distinct_filepath_cnt, count(1) statement_cnt
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC ORDER BY statement_cnt DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- summary of parse and transpilation
# MAGIC SELECT count(1) count, 
# MAGIC case 
# MAGIC   WHEN error_class is null THEN 'good' 
# MAGIC   WHEN strategy = 'ignore' THEN 'good'
# MAGIC   WHEN error_class = 'TABLE_OR_VIEW_NOT_FOUND' THEN 'probably ok'
# MAGIC   WHEN error_class = 'ParseException' THEN 'sqlglot parse issue'
# MAGIC   WHEN error_class = 'PARSE_SYNTAX_ERROR' THEN 'sqlglot optimize for Databricks'
# MAGIC   WHEN error_class = 'UNRESOLVED_ROUTINE' THEN 'risk'
# MAGIC   ELSE 'unknown' END status
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC group by 2
# MAGIC order by 1 desc

# COMMAND ----------


