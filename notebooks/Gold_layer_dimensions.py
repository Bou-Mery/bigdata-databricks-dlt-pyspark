# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

#df = spark.sql("SELECT * FROM workspace.silver.silver_flights")
#df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Parameters**

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # **Fetching Parameters & Creating Variables**

# COMMAND ----------

## Key Colums List
#key_cols_list = eval(dbutils.widgets.get("keycols"))
key_cols ="['passenger_id']"
key_cols_list = eval(key_cols)


## CDC Colums 
#cdc_col = eval(dbutils.widgets.get("cdccol"))
cdc_col = "modifiedDate"
## Back-Dated Refresh
# backdated_refresh = eval(dbutils.widgets.get("backdated_refresh"))
backdated_refresh = ""

## Source Object
#source_object = dbutils.widgets.get("source_object")
source_object = "silver_passengers"

## Source Schema
#source_schema = dbutils.widgets.get("source_schema")
source_schema ="silver"

## Target Schema 
#target_schema = dbutils.widgets.get("target_schema")
target_schema = "gold"

## Target Object
#target_object = dbutils.widgets.get("target_object")
target_object = "dim_passengers"


## Surrogate Key
surrogate_key = "passengers_dim_key"

# COMMAND ----------

# MAGIC %md
# MAGIC # **Incremental Data Ingestion**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Last Load Date

# COMMAND ----------

## No Back Dated Refresh
if len(backdated_refresh) == 0 :
    
    ## If Table Exists In The Destination
    if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}") :
        
        last_load = spark.sql(f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]
        
    else :
        last_load = "1900-01-01 00:00:00"

else : 
    last_load = backdated_refresh

## Test The Last Load 
last_load 
        

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} >= '{last_load}' ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Old VS New Records

# COMMAND ----------

key_columns_strings = ', '.join(key_cols_list)
key_columns_strings

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}") :

    ## Key Columns String For Incremental 
    key_columns_string_incremental = ', '.join(key_cols_list)

    df_tgt = spark.sql(f"SELECT {key_columns_string_incremental} , {surrogate_key} , create_date , update_date FROM workspace.{target_schema}.{target_object}")

else :
      ## Key Columns String For Initial 
      key_columns_string_init = [f" '' AS {i} " for i in key_cols_list]
      key_columns_string_init = ', '.join(key_columns_string_init)

      df_tgt = spark.sql(f"""SELECT {key_columns_string_init} , CAST('0' AS INT) AS {surrogate_key} ,  CAST('1900-01-01 00:00:00' AS timestamp ) AS create_date ,     CAST('1900-01-01 00:00:00' AS timestamp) AS update_date """)
    

# COMMAND ----------

df_tgt.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Join Condition**

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = tgt.{i}" for i in key_cols_list])
join_condition

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_tgt.createOrReplaceTempView("tgt")

df_join = spark.sql(f"""
           SELECT src.* ,
                  tgt.{surrogate_key},
                  tgt.create_date ,
                  tgt.update_date
            FROM src 
            LEFT JOIN tgt
            ON {join_condition}
           """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

## Old Records
df_old= df_join.filter(col(f'{surrogate_key}').isNotNull())

## New Records
df_new= df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

df_old.display()
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Enriching DataFrame**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing Old DataFrame**

# COMMAND ----------

df_old_enr = df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing New DataFrame**

# COMMAND ----------

df_new.display()

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}") :
    max_surrogate_key = spark.sql(f"""
          SELECT MAX({surrogate_key}) FROM workspace.{target_schema}.{target_object}""").collect()[0][0]
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
    .withColumn("create_date", current_timestamp())\
    .withColumn("update_date", current_timestamp())

else : 
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
    .withColumn("create_date", current_timestamp())\
    .withColumn("update_date", current_timestamp())






# COMMAND ----------

df_old_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Unioning Old & New  Records**

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)


# COMMAND ----------

# MAGIC %md
# MAGIC ## **UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"workspace.{target_schema}.{target_object}")
    dlt_obj.alias("tgt").merge(
        df_union.alias("src"),
        f"tgt.{surrogate_key} = src.{surrogate_key}"
    )\
    .whenMatchedUpdateAll(
        condition=f"src.{cdc_col} >= tgt.{cdc_col}"
    )\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_union.write.format("delta").mode("append").saveAsTable(f"workspace.{target_schema}.{target_object}")

# COMMAND ----------

spark.sql(f""" SELECT * FROM workspace.{target_schema}.{target_object}""").display()