# Databricks notebook source
# MAGIC %md
# MAGIC ## Install the Faker Library

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC Filter By Retention with Views

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
# MAGIC CREATE OR REPLACE VIEW person_info_vw
# MAGIC AS SELECT *
# MAGIC FROM person_info
# MAGIC WHERE record_dt >= (curdate() - INTERVAL '7' YEAR);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person_info_vw ORDER BY record_dt DESC;
