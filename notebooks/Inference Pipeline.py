# Databricks notebook source

dbutils.widgets.text("catalog_name", "redbus_demand_forecast")

catalog = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

test_df = spark.read.table(f"{catalog}.gold.test_ml")

test_pd = test_df.toPandas()

# COMMAND ----------

# MAGIC %pip install lightgbm

# COMMAND ----------

import mlflow

run_id = "8ad7f018a8324f91b26081e399e440ae"

model = mlflow.sklearn.load_model(f"runs:/{run_id}/redbus_model")

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

test_pd["predicted_seatcount"] = model.predict(test_pd[features])

# COMMAND ----------

spark.createDataFrame(test_pd) \
.write.format("delta") \
.mode("overwrite") \
.saveAsTable("redbus_demand_forecast.gold.predictions")

# COMMAND ----------

display(spark.table("redbus_demand_forecast.gold.predictions"))

# COMMAND ----------

