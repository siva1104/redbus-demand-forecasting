# Databricks notebook source
dbutils.widgets.text("catalog_name", "redbus_demand_forecast")

catalog = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

train_df = spark.read.table(f"{catalog}.gold.train_ml")
test_df = spark.read.table(f"{catalog}.gold.test_ml")

# COMMAND ----------

train_pd = train_df.toPandas()
test_pd = test_df.toPandas()

# COMMAND ----------

target = "final_seatcount"

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

train_data = train_pd[train_pd["month"] <= 9]
val_data = train_pd[train_pd["month"] > 9]

X_train = train_data[features]
y_train = train_data[target]

X_val = val_data[features]
y_val = val_data[target]

# COMMAND ----------

# MAGIC %pip install lightgbm

# COMMAND ----------

import lightgbm as lgb
import numpy as np
from sklearn.metrics import mean_squared_error

model = lgb.LGBMRegressor(
    learning_rate=0.05,
    n_estimators=500,
    max_depth=6
)

model.fit(X_train, y_train)

val_preds = model.predict(X_val)

rmse = np.sqrt(mean_squared_error(y_val, val_preds))

print("Validation RMSE:", rmse)

# COMMAND ----------

import mlflow
import mlflow.sklearn

mlflow.set_experiment("/Workspace/Users/sivakumarr207@gmail.com/Codebasics_hackathon")
with mlflow.start_run():

    model = lgb.LGBMRegressor(
        learning_rate=0.05,
        n_estimators=500,
        max_depth=6
    )

    model.fit(X_train, y_train)

    val_preds = model.predict(X_val)

    rmse = np.sqrt(mean_squared_error(y_val, val_preds))

    mlflow.log_param("learning_rate", 0.05)
    mlflow.log_param("n_estimators", 500)
    mlflow.log_param("max_depth", 6)

    mlflow.log_metric("rmse", rmse)

    mlflow.sklearn.log_model(model, "redbus_model")

    print("Logged RMSE:", rmse)

# COMMAND ----------

test_preds = model.predict(test_pd[features])

test_pd["predicted_seatcount"] = test_preds

# COMMAND ----------

spark.createDataFrame(test_pd) \
.write.format("delta") \
.mode("overwrite") \
.saveAsTable("redbus_demand_forecast.gold.predictions")

# COMMAND ----------

spark.sql("USE CATALOG redbus_demand_forecast")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

display(spark.table("redbus_demand_forecast.gold.predictions"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

