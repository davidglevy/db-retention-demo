# Databricks notebook source
# MAGIC %md
# MAGIC # Row Level Filter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the Faker Library

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS demo.retention_examples;
# MAGIC
# MAGIC USE demo.retention_examples;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Lab Reset Functionality
# MAGIC
# MAGIC We use a consistent seed in combination with the Faker library to create the same base person_info and person_info_history tables.

# COMMAND ----------

from lab_setup import reset_lab

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create The Data

# COMMAND ----------

reset_lab(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person_info ORDER BY record_dt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Delete by Date
# MAGIC
# MAGIC Here we run the most basic example where we delete by date.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE person_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE person_info DROP ROW FILTER;

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS retention_check;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION retention_check(id BIGINT, record_dt DATE)
# MAGIC RETURNS BOOLEAN
# MAGIC   RETURN (record_dt >= (curdate() - INTERVAL '7' YEAR))
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION test_check(id BIGINT, record_dt BIGINT)
# MAGIC -- RETURNS BOOLEAN
# MAGIC --  RETURN FALSE;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE person_info
# MAGIC SET ROW FILTER retention_check ON (id, record_dt)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE person_info ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person_info ORDER BY record_dt DESC;
