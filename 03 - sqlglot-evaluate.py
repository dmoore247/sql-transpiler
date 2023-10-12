# Databricks notebook source
# MAGIC %md # SQLGlot Demo - Evaluate bulk conversion results

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG douglas_moore

# COMMAND ----------

# MAGIC %md ## Summarize and aggregate results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- summarize results
# MAGIC SELECT count(1) count, statement_type, strategy, error_class, sqlglot, cast(insert_dt as DATE)
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC GROUP BY 2,3,4,5,6
# MAGIC ORDER BY 1 DESC, 2

# COMMAND ----------

# MAGIC %md ## Errors by file

# COMMAND ----------

# MAGIC %sql
# MAGIC -- errors organized by file
# MAGIC SELECT count(1) cnt, file_path, error_class
# MAGIC FROM douglas_moore.sqlglot.project1 a
# MAGIC GROUP BY file_path, error_class
# MAGIC ORDER BY cnt DESC

# COMMAND ----------

# MAGIC %md ## Successful transpilations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 100% successful transpilations
# MAGIC select * from douglas_moore.sqlglot.project1
# MAGIC where error_class is null

# COMMAND ----------

# MAGIC %md ## Total statements

# COMMAND ----------

# MAGIC %sql
# MAGIC -- total statements found
# MAGIC SELECT count(DISTINCT file_path) distinct_filepath_cnt, count(1) statement_cnt
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC ORDER BY statement_cnt DESC

# COMMAND ----------

# MAGIC %md ## Summary of parse and transpilation

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

# MAGIC %sql
# MAGIC WITH parse_classes AS (
# MAGIC SELECT 
# MAGIC   count(1) cnt, 
# MAGIC   case 
# MAGIC     WHEN error_class is null THEN '1. good' 
# MAGIC     WHEN strategy = 'ignore' THEN '1. good'
# MAGIC     WHEN error_class = 'TABLE_OR_VIEW_NOT_FOUND' THEN '2. likely good'
# MAGIC     WHEN error_class = 'ParseException' THEN '3. optimize sqlglot for Databricks'
# MAGIC     WHEN error_class = 'PARSE_SYNTAX_ERROR' THEN '3. optimize sqlglot for Databricks'
# MAGIC     WHEN error_class = 'UNRESOLVED_ROUTINE' THEN '4. custom function'
# MAGIC     ELSE 'unknown' END status
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC group by status
# MAGIC order by cnt desc
# MAGIC ),
# MAGIC totals AS (
# MAGIC   select sum(cnt) as total FROM parse_classes
# MAGIC ),
# MAGIC cte AS (
# MAGIC     SELECT 
# MAGIC       status, cnt, totals.total, format_number(100.* cnt/total, '###.#') as pct
# MAGIC     FROM parse_classes, totals
# MAGIC )
# MAGIC select status, cnt, pct
# MAGIC FROM cte
# MAGIC order by status

# COMMAND ----------

1.0 - (35-17)/(110+8+35+1)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH parse_classes AS (
# MAGIC SELECT 
# MAGIC   count(1) cnt, 
# MAGIC   statement_type,
# MAGIC   case 
# MAGIC     WHEN error_class is null THEN '1. good' 
# MAGIC     WHEN strategy = 'ignore' THEN '1. good'
# MAGIC     WHEN error_class = 'TABLE_OR_VIEW_NOT_FOUND' THEN '2. likely good'
# MAGIC     WHEN error_class = 'ParseException' THEN '3. optimize sqlglot for Databricks'
# MAGIC     WHEN error_class = 'PARSE_SYNTAX_ERROR' THEN '3. optimize sqlglot for Databricks'
# MAGIC     WHEN error_class = 'UNRESOLVED_ROUTINE' THEN '4. custom function'
# MAGIC     ELSE 'unknown' END status
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC group by status, statement_type
# MAGIC order by cnt desc
# MAGIC ),
# MAGIC totals AS (
# MAGIC   select sum(cnt) as total FROM parse_classes
# MAGIC ),
# MAGIC cte AS (
# MAGIC     SELECT 
# MAGIC       status, statement_type, cnt, totals.total, format_number(100.* cnt/total, '###.#') as pct
# MAGIC     FROM parse_classes, totals
# MAGIC )
# MAGIC select status, statement_type, cnt, pct
# MAGIC FROM cte
# MAGIC order by status, cnt DESC, statement_type

# COMMAND ----------


