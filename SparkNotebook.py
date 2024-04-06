# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read and load a CSV File

# COMMAND ----------

fire_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")
     

# COMMAND ----------

fire_df.display(10)

# COMMAND ----------

fire_df.createOrReplaceGlobalTempView("fire_df_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.fire_df_view

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing of spaces from the column names :

# COMMAND ----------

renamed_fire_df = fire_df \
    .withColumnRenamed("Call Number", "CallNumber") \
    .withColumnRenamed("Unit ID", "UnitID") \
    .withColumnRenamed("Incident Number", "IncidentNumber") \
    .withColumnRenamed("Call Date", "CallDate") \
    .withColumnRenamed("Watch Date", "WatchDate") \
    .withColumnRenamed("Call Final Disposition", "CallFinalDisposition") \
    .withColumnRenamed("Available DtTm", "AvailableDtTm") \
    .withColumnRenamed("Zipcode of Incident", "Zipcode") \
    .withColumnRenamed("Station Area", "StationArea") \
    .withColumnRenamed("Final Priority", "FinalPriority") \
    .withColumnRenamed("ALS Unit", "ALSUnit") \
    .withColumnRenamed("Call Type Group", "CallTypeGroup") \
    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch") \
    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
    .withColumnRenamed("Supervisor District", "SupervisorDistrict")
     


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Performing transformation on the DataType from string to date and timestamp

# COMMAND ----------

fire_df_dates=renamed_fire_df \
    .withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy")) \
    .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy")) \
    .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("Delay", round("Delay", 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q1. How many distinct types of calls were made to the Fire Department?

# COMMAND ----------

distinct_call=fire_df_dates.where("Calltype is not null").select("CallType").distinct()
print(distinct_call.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q2. What were distinct types of calls made to the Fire Department?

# COMMAND ----------

distinct_typecalls=fire_df_dates.where("Calltype is not null").select("Calltype").distinct()
print(distinct_typecalls.display())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q3. Find out all response for delayed times greater than 5 mins?

# COMMAND ----------

delay_responses=fire_df_dates.select("Delay").where("Delay > 5" )
print(delay_responses.display())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q4. What were the most common call types?

# COMMAND ----------

common=fire_df_dates.select("Calltype").groupBy("Calltype").count().orderBy("count", ascending=False)
print(common.display())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q5. What zip codes accounted for most common calls?
# MAGIC

# COMMAND ----------

zipcodes=fire_df_dates.select("calltype","zipcode").groupBy("calltype","zipcode").count().orderBy("count",ascending=False)
zipcodes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

# COMMAND ----------

zipcode=fire_df_dates.select("zipcode","neighborhood").where("zipcode==94102" or "zipcode==94103").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q7. What was the sum of all call alarms, average, min, and max of the call response times?

# COMMAND ----------

response=fire_df_dates.select(sum("NumAlarms"),avg("Delay"),min("Delay"),max("Delay")).display()
