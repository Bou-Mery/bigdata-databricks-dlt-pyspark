# Databricks notebook source
# MAGIC %md
# MAGIC # **Parameters**

# COMMAND ----------

## Source Object
source_object = "silver_bookings"

## Source Schema
source_schema ="silver"

## Target Schema 
target_schema = "gold"

## Target Object
target_object = "fact_bookings"

## CDC Colums 
cdc_col = "modifiedDate"
## Back-Dated Refresh
backdated_refresh = ""

#Source Fact Table 
fact_table = f"workspace.{source_schema}.{source_object}"

# Fact Key Columns List
fact_key_cols = ["passengers_dim_key", "flights_dim_key" , "airports_dim_key" , "booking_date"]



# COMMAND ----------

dimensions = [
    {
        "table": "workspace.gold.dim_passengers",
        "alias": "dim_passengers",
        "join_keys": [("passenger_id", "passenger_id")],
        "dim_key":"passengers_dim",
         
    } ,
     {
        "table": "workspace.gold.dim_flights",
        "alias": "dim_flights",
        "join_keys": [("flight_id", "flight_id")],
        "dim_key":"flights_dim",
    },
     {
        "table": "workspace.gold.dim_airports",
        "alias": "dim_airports",
        "join_keys": [("airport_id", "airport_id")],
        "dim_key":"airports_dim",
    }
]

# Columns you want to keep from fact table 
fact_columns =["amount" , "booking_date" , "modifiedDate"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Last Load Date**

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

# MAGIC %md
# MAGIC ## **Dynamic Fact Query (Bring Keys)**

# COMMAND ----------

def generate_fact_query_incremental(fact_table , dimensions , fact_columns , cdc_col , last_load ) :
    fact_alias = "f"

    #Base columns to select
    base_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    #Join conditions
    join_conditions = []
    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        dim_key = dim["dim_key"]
        table_name = table_full.split('.')[-1]
        surrogate_key = f"{alias}.{dim_key}_key"
        base_cols.append(surrogate_key)

        #Build On Clause
        on_conditions = [
            f"{fact_alias}.{fk} = {alias}.{dk} " for fk , dk in dim["join_keys"] 
        ]

        join_clause=f"LEFT JOIN {table_full} AS {alias} ON " + " AND ".join(on_conditions)
        join_conditions.append(join_clause)

    # Final Select and Join Clauses
    select_clause = ",\n    ".join(base_cols)
    join_clause = "\n".join(join_conditions)

    # Where Clause for Incremental Filtering
    where_clause = f"{fact_alias}.{cdc_col} >= DATE('{last_load}')"

    # Final Query
    query = f"""
        SELECT
            {select_clause}
        FROM {fact_table} AS {fact_alias}
            {join_clause}
        WHERE {where_clause}
    """.strip()

    return query


# COMMAND ----------

query = generate_fact_query_incremental(fact_table , dimensions , fact_columns , cdc_col , last_load )


# COMMAND ----------

print(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Fact Table**

# COMMAND ----------

df_fact = spark.sql(query)
display(df_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Fact Keys Condition**

# COMMAND ----------

fact_keys_cond = " AND ".join([f"tgt.{col} = src.{col}" for col in fact_key_cols])
fact_keys_cond


# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"workspace.{target_schema}.{target_object}")
    dlt_obj.alias("tgt").merge(
        df_fact.alias("src"),
        fact_keys_cond
    )\
    .whenMatchedUpdateAll(
        condition=f"src.{cdc_col} >= tgt.{cdc_col}"
    )\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_fact.write.format("delta").mode("append").saveAsTable(f"workspace.{target_schema}.{target_object}")

# COMMAND ----------

df_fact.display()