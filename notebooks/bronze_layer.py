# Databricks notebook source
dbutils.widgets.text("catalog_name", "redbus_demand_forecast")

catalog = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

base_path = "s3a://redbus-demand-data"

# COMMAND ----------

train_df = spark.read.csv(
    f"{base_path}/train.csv",
    header=True,
    inferSchema=True
)

test_df = spark.read.csv(
    f"{base_path}/test_8gqdJqH.csv",
    header=True,
    inferSchema=True
)

transactions_df = spark.read.csv(
    f"{base_path}/transactions.csv",
    header=True,
    inferSchema=True
)

# COMMAND ----------

display(train_df)
display(transactions_df)
display(test_df)


# COMMAND ----------

train_df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("train")

# COMMAND ----------

test_df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("test")

# COMMAND ----------

transactions_df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("transactions")

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

display(spark.table("train"))

# COMMAND ----------



# COMMAND ----------

