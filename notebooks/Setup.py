# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.raw_volume
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw_data")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw_data/bookings")
dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw_data/flights")
dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw_data/customers")
dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw_data/airports")

# COMMAND ----------

# Uplouding files for each folder

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.bronze ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.silver ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.gold ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronze_volume/flights/data/`

# COMMAND ----------

# MAGIC %md
# MAGIC