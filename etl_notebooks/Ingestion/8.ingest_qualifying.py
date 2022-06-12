# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimestampType, DateType, FloatType

# COMMAND ----------

qualifying_schema = StructType(fields ={
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
      StructField("q3", StringType(), True)
    
    
})

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split_*.json")

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp, lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("raceId","race_id") \
                        .withColumnRenamed("driverId","driver_id") \
                        .withColumnRenamed("constructorId","constructor_id") \
                        .withColumn("ingestion_date", current_timestamp()) \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.qualify_id = src.qualify_id"
merge_delta_data(final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")
