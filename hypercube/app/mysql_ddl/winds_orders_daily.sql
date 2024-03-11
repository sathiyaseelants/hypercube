CREATE DATABASE wind_db;

create schema wind_aggr_dbo;

CREATE TABLE wind_orders_daily (
    order_date DATE,
    max_SpnGeneration DOUBLE,
    mean_max_SpnGeneration DOUBLE,
    Total_ExecutedVolume DOUBLE
) PARTITION BY RANGE (YEAR(order_date)) (
    PARTITION p0 VALUES LESS THAN (2023),
    PARTITION p1 VALUES LESS THAN (2024),
    PARTITION p2 VALUES LESS THAN (2025),
    PARTITION p3 VALUES LESS THAN (2026)
);

CREATE TABLE wind_orders_weekly (
    order_week DATE,
    max_SpnGeneration DOUBLE,
    mean_max_SpnGeneration DOUBLE,
    Total_ExecutedVolume DOUBLE
);