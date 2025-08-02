# Databricks notebook source
# MAGIC %md
# MAGIC ### INCREMENTAL DATA INGESTION

# COMMAND ----------

dbutils.widgets.text("src" ,"")

# COMMAND ----------

src_value = dbutils.widgets.get("src")


# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format" , "csv") \
    .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronze_volume/{src_value}/checkpoint") \
    .option("cloudFiles.schemaEvolutionMode" ,"rescue") \
    .load(f"/Volumes/workspace/raw/raw_volume/raw_data/{src_value}/")

# COMMAND ----------

df.writeStream.format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", f"/Volumes/workspace/bronze/bronze_volume/{src_value}/checkpoint") \
   .option("path", f"/Volumes/workspace/bronze/bronze_volume/{src_value}/data") \
    .start()
   