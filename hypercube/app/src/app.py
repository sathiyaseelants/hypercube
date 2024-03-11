#!/usr/bin/env python3

import os
import sys

file_dir = os.getcwd()
sys.path.append(file_dir)

from src.data_cleaning import clean_data
from transform import calculate_rolling_median, build_daily_aggregate_view, build_weekly_aggregate_view
from src.database import write_to_mysql
from src.rest_api import put_wind_orders
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


from pyspark.sql.functions import col, round, explode, to_date, count, when, weekofyear

appName = "HyperCube"
master = "local"

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .getOrCreate()

    # Read CSV files
    df_wind = spark.read.csv("/mnt/d/DE/DE/data/bmrs_wind_forecast_pair.csv", header=True, inferSchema=True, nanValue="NULL", mode="DROPMALFORMED")
    df_wind = df_wind.filter(F.col("initialForecastSpnGeneration").rlike("^\\d+\\.?\\d*$")) \
                     .withColumn("initialForecastSpnGeneration", df_wind["initialForecastSpnGeneration"].cast("double")) \
                     .withColumn("ForecastCommenceDate", F.to_date(F.col("initialForecastPublishingPeriodCommencingTime"), "dd/MM/yyyy HH:mm")) \
                     .withColumnRenamed("efa", "wind_efa")
    
    df_orders = spark.read.json("/mnt/d/DE/DE/data/linear_orders_raw.json", multiLine=True)
    df_exploded = df_orders.select(F.explode("result.records").alias("record"))
    df_orders = df_exploded.select("record.*") \
                           .withColumn("DeliveryStartDate", F.to_date(F.col("DeliveryStart"))) \
                           .withColumnRenamed("efa", "orders_efa")

    # Clean data
    df_wind_cleaned = clean_data(df_wind)
    df_orders_cleaned = clean_data(df_orders)

    # Aggregate data
    df_max_SpnGeneration = df_wind_cleaned.filter(F.col("ForecastCommenceDate").isNotNull() & F.col("initialForecastSpnGeneration").isNotNull()) \
                     .groupBy('ForecastCommenceDate') \
                     .agg(F.max('initialForecastSpnGeneration_imputed').alias('max_SpnGeneration'))
    df_total_executed_volume = df_orders_cleaned.filter(F.col("DeliveryStartDate").isNotNull() & F.col("ExecutedVolume").isNotNull()) \
                         .groupBy('DeliveryStartDate') \
                         .agg(F.sum('ExecutedVolume').alias('Total_ExecutedVolume'))

    # Join data
    df_wind_orders = df_total_executed_volume.join(df_max_SpnGeneration, df_total_executed_volume['DeliveryStartDate'] == df_max_SpnGeneration['ForecastCommenceDate']) \
                                                   .select('DeliveryStartDate', 'max_SpnGeneration', 'Total_ExecutedVolume')

    # Feature Engineering
    df_with_rolling_median = calculate_rolling_median(df_wind_orders, "DeliveryStartDate", ["max_SpnGeneration", "Total_ExecutedVolume"], 6)
    df_daily_aggregate_view = build_daily_aggregate_view(df_with_rolling_median)
    df_weekly_aggregate_view = build_weekly_aggregate_view(df_with_rolling_median)

    df_daily_aggregate_view.show(5)
    df_weekly_aggregate_view.show(5)

    # Save to Database
    write_to_mysql(df_daily_aggregate_view, "wind_orders_daily")
    write_to_mysql(df_weekly_aggregate_view, "wind_orders_weekly")

    # Simulate API Call
    put_wind_orders(df_wind_orders)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
