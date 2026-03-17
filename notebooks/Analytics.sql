-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.sql("USE CATALOG redbus_demand_forecast")
-- MAGIC spark.sql("USE SCHEMA gold")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.table("predictions")

-- COMMAND ----------

SELECT srcid, destid,
AVG(predicted_seatcount) AS avg_demand
FROM redbus_demand_forecast.gold.predictions
GROUP BY srcid, destid
ORDER BY avg_demand DESC
LIMIT 10;

-- COMMAND ----------

SELECT src_region,
AVG(predicted_seatcount) AS demand
FROM redbus_demand_forecast.gold.predictions
GROUP BY src_region
ORDER BY demand DESC;

-- COMMAND ----------

SELECT weekend_flag,
AVG(predicted_seatcount)
FROM redbus_demand_forecast.gold.predictions
GROUP BY weekend_flag;

-- COMMAND ----------

SELECT month,
AVG(predicted_seatcount)
FROM redbus_demand_forecast.gold.predictions
GROUP BY month
ORDER BY month;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

