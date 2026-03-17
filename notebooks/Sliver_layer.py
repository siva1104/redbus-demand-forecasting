# Databricks notebook source
dbutils.widgets.text("catalog_name", "redbus_demand_forecast")

catalog = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

train_df = spark.read.table(f"{catalog}.bronze.train")
test_df = spark.read.table(f"{catalog}.bronze.test")
transactions_df = spark.read.table(f"{catalog}.bronze.transactions")

# COMMAND ----------

for df_name in ["train_df", "test_df", "transactions_df"]:
    globals()[df_name] = globals()[df_name].toDF(
        *[c.lower().strip() for c in globals()[df_name].columns]
    )

# COMMAND ----------


from pyspark.sql.functions import to_date

train_df = train_df.withColumn("doj", to_date("doj"))
test_df = test_df.withColumn("doj", to_date("doj"))

transactions_df = transactions_df \
    .withColumn("doj", to_date("doj")) \
    .withColumn("doi", to_date("doi"))

# COMMAND ----------

transactions_df = transactions_df.fillna({
    "cumsum_searchcount": 0,
    "cumsum_seatcount": 0
})

# COMMAND ----------

txn_df = transactions_df.filter("dbd >= 15")

# COMMAND ----------

from pyspark.sql.functions import max, avg, last

features_df = txn_df.groupBy("srcid", "destid", "doj").agg(
    max("cumsum_seatcount").alias("seat_max_15"),
    avg("cumsum_seatcount").alias("seat_mean_15"),
    last("cumsum_seatcount").alias("seat_last_15"),
    
    max("cumsum_searchcount").alias("search_max_15"),
    avg("cumsum_searchcount").alias("search_mean_15"),
    last("cumsum_searchcount").alias("search_last_15"),
    
    last("srcid_region").alias("src_region"),
    last("destid_region").alias("dest_region"),
    last("srcid_tier").alias("src_tier"),
    last("destid_tier").alias("dest_tier")
)

# COMMAND ----------

silver_train = train_df.join(
    features_df,
    on=["srcid", "destid", "doj"],
    how="left"
)

# COMMAND ----------

silver_test = test_df.join(
    features_df,
    on=["srcid", "destid", "doj"],
    how="left"
)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear

silver_train = silver_train \
    .withColumn("year", year("doj")) \
    .withColumn("month", month("doj")) \
    .withColumn("day", dayofmonth("doj")) \
    .withColumn("dayofweek", dayofweek("doj")) \
    .withColumn("weekofyear", weekofyear("doj"))

silver_test = silver_test \
    .withColumn("year", year("doj")) \
    .withColumn("month", month("doj")) \
    .withColumn("day", dayofmonth("doj")) \
    .withColumn("dayofweek", dayofweek("doj")) \
    .withColumn("weekofyear", weekofyear("doj"))

# COMMAND ----------

silver_train.write.format("delta") \
.mode("overwrite") \
.saveAsTable("train_features")

silver_test.write.format("delta") \
.mode("overwrite") \
.saveAsTable("test_features")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE redbus_demand_forecast.silver.train_features
# MAGIC ZORDER BY (srcid, destid);
# MAGIC
# MAGIC OPTIMIZE redbus_demand_forecast.silver.test_features
# MAGIC ZORDER BY (srcid, destid);