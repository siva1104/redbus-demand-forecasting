# 🚍 RedBus Demand Forecasting using Databricks

## 📌 Problem Statement
Forecast bus demand (final seat count) 15 days before journey.

## 🏗️ Architecture
![Architecture](images/architecture.png)

## ⚙️ Tech Stack
- Databricks
- PySpark
- Delta Lake
- MLflow
- LightGBM

## 🔄 Pipeline
S3 → Bronze → Silver → Gold → ML → MLflow → Inference → Dashboard → Orchestration

## 🧠 Features
- Booking Intensity
- Demand Pressure
- Search Momentum
- Route Demand

## 🤖 Model
- LightGBM
- Evaluation Metric: RMSE

## 📊 Analytics
- Top Routes
- Weekend vs Weekday Demand
- Tier-based Demand
- Region Insights

## 🚀 Business Impact
- Dynamic Pricing
- Route Optimization
- Demand Forecasting

## 🔁 Orchestration
Automated using Databricks Jobs

## 📌 Author
Sivakumar
