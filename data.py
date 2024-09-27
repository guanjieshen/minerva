# Databricks notebook source
delays_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("/databricks-datasets/flights/departuredelays.csv")
)
airports_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferschema", "true")
    .option("delimiter", "\t")
    .load("/databricks-datasets/flights/airport-codes-na.txt")
)

# COMMAND ----------

delays_df.write.format("delta").option(
    "path", "abfss://guanjie@oneenvadls.dfs.core.windows.net/flights/delays"
).saveAsTable("gshen_uniform.flights.delays")

# COMMAND ----------

airports_df.write.format("delta").option(
    "path", "abfss://guanjie@oneenvadls.dfs.core.windows.net/flights/airports"
).saveAsTable("gshen_uniform.flights.airports")

# COMMAND ----------

# MAGIC %md ### Enable Uniform

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gshen_uniform.flights.airports SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gshen_uniform.flights.airports SET TBLPROPERTIES(
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gshen_uniform.flights.delays SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gshen_uniform.flights.delays SET TBLPROPERTIES(
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg');
