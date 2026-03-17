# Databricks notebook source
dbutils.widgets.text("catalog_name", "redbus_demand_forecast")

catalog = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

train_df = spark.read.table(f"{catalog}.silver.train_features")
test_df = spark.read.table(f"{catalog}.silver.test_features")

# COMMAND ----------

from pyspark.sql.functions import col

train_df = train_df.withColumn(
    "booking_intensity",
    col("seat_last_15") / (col("search_last_15") + 1)
)

test_df = test_df.withColumn(
    "booking_intensity",
    col("seat_last_15") / (col("search_last_15") + 1)
)

# COMMAND ----------

train_df = train_df.withColumn(
    "demand_pressure",
    col("seat_max_15") - col("seat_mean_15")
)

test_df = test_df.withColumn(
    "demand_pressure",
    col("seat_max_15") - col("seat_mean_15")
)

# COMMAND ----------

train_df = train_df.withColumn(
    "search_momentum",
    col("search_max_15") - col("search_mean_15")
)

test_df = test_df.withColumn(
    "search_momentum",
    col("search_max_15") - col("search_mean_15")
)

# COMMAND ----------

from pyspark.sql.functions import avg

route_popularity = train_df.groupBy("srcid", "destid") \
    .agg(avg("final_seatcount").alias("route_avg_demand"))

train_df = train_df.join(route_popularity, ["srcid","destid"], "left")
test_df = test_df.join(route_popularity, ["srcid","destid"], "left")

# COMMAND ----------

train_df = train_df.withColumn(
    "weekend_flag",
    (col("dayofweek").isin([1,7])).cast("int")
)

test_df = test_df.withColumn(
    "weekend_flag",
    (col("dayofweek").isin([1,7])).cast("int")
)

# COMMAND ----------

features = [
    "seat_last_15",
    "search_last_15",
    "booking_intensity",
    "demand_pressure",
    "search_momentum",
    "route_avg_demand",
    "weekend_flag",
    "month",
    "dayofweek"
]

# COMMAND ----------

train_df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("train_ml")

# COMMAND ----------

test_df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("test_ml")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE redbus_demand_forecast.gold.train_ml
# MAGIC ZORDER BY (srcid, destid);
# MAGIC
# MAGIC OPTIMIZE redbus_demand_forecast.gold.test_ml
# MAGIC ZORDER BY (srcid, destid);

# COMMAND ----------

