# Databricks notebook source
# MAGIC %md
# MAGIC # Delete by Date
# MAGIC
# MAGIC This is likely the simplest method to implement record retention by date.
# MAGIC
# MAGIC On the face of it, this is a good approach, however it is difficult/Impossible when combined with Data Vault or Slowly Changing dimensions and we'd need special treatments for continuity at boundary rows.
# MAGIC
# MAGIC Further, the Deletion/Vacuum would require us to halt Streaming updates. This approach is further complicated as “source systems” still have deleted data, such as a "delete flag". We'd need to ensure that incoming data is also excluded.
# MAGIC
# MAGIC A final complication here is that deletion must be paired with a VACUUM operation (to delete table history) otherwise users can easily bypass retention periods. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the Faker Library
# MAGIC
# MAGIC We install the faker library required by our lab_setup Python module.

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
# MAGIC ## Run the Delete by Date
# MAGIC
# MAGIC Here we run the most basic example where we delete by date.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM person_info WHERE record_dt < (curdate() - INTERVAL '7' YEAR);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person_info ORDER BY record_dt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show the Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY person_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the Data Using Time Travel
# MAGIC Check whether data exists in history.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person_info VERSION AS OF 1 ORDER BY record_dt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Vacuum to Delete the History
# MAGIC
# MAGIC We're going to utilise the vacuum command to delete the prior versions - however we need to disable the retentionDuractionCheck for our test - this SHOULD NOT BE SET SO LOW FOR PRODUCTION. We're only doing this for our tests. Instead set this to higher period that won't break any writers (e.g. 1 day or more)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM person_info RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Current Version Unaffected
# MAGIC
# MAGIC We've run the VACUUM with an artificially low number - check the current version is still present.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person_info ORDER BY record_dt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check our Table History
# MAGIC We can still see our table history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY person_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Old Versions Removed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person_info VERSION AS OF 1 ORDER BY record_dt DESC;
