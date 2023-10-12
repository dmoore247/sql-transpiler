# Databricks notebook source
# MAGIC %md ## Summary
# MAGIC - `sqlglot` does save a lot of complex regex
# MAGIC - `sqlglot` is not a cure all
# MAGIC - `sqlglot` strategy will require investment in:
# MAGIC   - The controller code, the user interface into transpiling
# MAGIC   - Optimizing the transpilation for Databricks
# MAGIC   - Additional TSQL cases that don't parse
# MAGIC
# MAGIC ### This controler and demo offers insight into
# MAGIC - Measurable competency of the open source transpilation
# MAGIC - Indentify areas where investment will pay off
# MAGIC
# MAGIC ### Right now we have a trade off....
# MAGIC - LLMs with humans can crank through a SSA request in a reasonable amount of time
# MAGIC - Parsers & transpilers are 60-80% reliable but handle large volumes
# MAGIC
# MAGIC ### Where to next?
# MAGIC - Apply this measurement framework to the Project Snowmelt and Snowflake transcompilation
# MAGIC - Apply this measurement framework to profilers (transpile and then evaluate complexity)

# COMMAND ----------


