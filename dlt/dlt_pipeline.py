from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt 


## Bookings Data
@dlt.table(
    name = "stage_bookings"
)
def stage_booking():
    df = spark.read.format("delta") \
        .load("/Volumes/workspace/bronze/bronze_volume/bookings/data")
    return df


@dlt.view(
    name= "transformed_bookings"
)
def transformed_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount" ,col("amount").cast(DoubleType())) \
        .withColumn("modifiedDate" , current_timestamp())\
        .withColumn("booking_date" , to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df
               

rules = {
    "rule1" : "booking_id IS NOT NULL" ,
    "rule2" : "passenger_id IS NOT NULL" 
}

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_booking() : 
    df = spark.readStream.table("transformed_bookings")    
    return df

####################################################################################################

## Flights Data
@dlt.view(
    name = "transformed_flights"
)
def transformed_flights() :
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronze_volume/flights/data")
    df = df.withColumn("modifiedDate" , current_timestamp())\
        .drop("_rescued_data") 
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
  target = "silver_flights",
  source = "transformed_flights",
  keys = ["flight_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)

####################################################################################################

## Passsangers Data
@dlt.view(
    name = "transformed_passengers"
)
def transformed_flights() :
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronze_volume/customers/data")
    df = df.withColumn("modifiedDate" , current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
  target = "silver_passengers",
  source = "transformed_passengers",
  keys = ["passenger_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)

####################################################################################################

## Airports Data
@dlt.view(
    name = "transformed_airports"
)
def transformed_flights() :
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronze_volume/airports/data")
    df = df.withColumn("modifiedDate" , current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
  target = "silver_airports",
  source = "transformed_airports",
  keys = ["airport_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)


####################################################################################################

## Silver Business View

@dlt.table(
    name ="silver_business"
)
def silver_business():
    df = spark.readStream.table("silver_bookings") \
        .join(spark.readStream.table("silver_flights"), ["flight_id"]) \
        .join(spark.readStream.table("silver_passengers" ,), ["passenger_id"])\
        .join(spark.readStream.table("silver_airports"), ["airport_id"])\
        .drop("modifiedDate")

    return df
       