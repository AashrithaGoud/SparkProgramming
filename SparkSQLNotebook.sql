-- Databricks notebook source
create database if not exists fire_db;

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/fire_db.db

-- COMMAND ----------

create table if not exists fire_db.fire_table_data(
  CallNumber integer,
  UnitID string,
  IncidentNumber integer,
  CallType string,
  CallDate string,
  WatchDate string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  Zipcode integer,
  Battalion string,
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  FinalPriority integer,
  ALSUnit boolean,
  CallTypeGroup string,
  NumAlarms integer,
  UnitType string,
  UnitSequenceInCallDispatch integer,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  Neighborhood string,
  Location string,
  RowID string,
  Delay float
) 

-- COMMAND ----------

insert into fire_db.fire_table_data
select * from global_temp.fire_df_view;

-- COMMAND ----------

select * from fire_db.fire_table_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q1. How many distinct types of calls were made to the Fire Department?

-- COMMAND ----------

select count(distinct calltype) as count_of_distinct_calltypes from fire_db.fire_table_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q2. What were distinct types of calls made to the Fire Department?

-- COMMAND ----------

select distinct CallType from fire_db.fire_table_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q3. Find out all response for delayed times greater than 5 mins?
-- MAGIC

-- COMMAND ----------

select CallNumber, Delay from fire_db.fire_table_data where Delay> 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q4. What were the most common call types?

-- COMMAND ----------

select calltype,count(*) as distinct_count_calltypes from fire_db.fire_table_data group by Calltype order by distinct_count_calltypes desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q5. What zip codes accounted for most common calls?

-- COMMAND ----------

select calltype, zipcode, count(*) as most_common_calls from fire_db.fire_table_data group by calltype, zipcode order by most_common_calls desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

-- COMMAND ----------

select zipcode, Neighborhood from fire_db.fire_table_data where zipCode == 94102 or zipCode == 94103;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Q7. What was the sum of all call alarms, average, min, and max of the call response times?

-- COMMAND ----------

select sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
from fire_db.fire_table_data
