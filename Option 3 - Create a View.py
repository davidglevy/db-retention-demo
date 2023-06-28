# Databricks notebook source
# MAGIC %md
# MAGIC # Create a View
# MAGIC This option looks at whether we can create a view for user consumption, which also necessitates us hiding
# MAGIC the data in the source table.
# MAGIC
# MAGIC This option is realtively easy however if we do this, users will be denied access to time travel and the policy is only effective if users do not receive access to the underlying table. We also need to ensure users have BROWSE permission on the underlying table to see lineage/metadata which may help them understand validity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the Faker Library

# COMMAND ----------

# MAGIC %pip install faker

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
# MAGIC ## Create our View
# MAGIC
# MAGIC Now we create a view with our retention policy.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW person_info_vw
# MAGIC AS SELECT *
# MAGIC FROM person_info
# MAGIC WHERE record_dt >= (curdate() - INTERVAL '7' YEAR);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person_info_vw ORDER BY record_dt DESC;
