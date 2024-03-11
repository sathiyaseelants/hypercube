
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_rolling_median(df, x_axis, y_axis, window_size):
    """
    This function calculates the rolling median for the input columns based on the input window size.
    :param df: A PySpark DataFrame
    :param x_axis: Timestamp series column for median calculation
    :param y_axis: List of columns for median calculation
    :param window_size: Time window size in hours for rolling median calculation
    :return: DataFrame with rolling median columns added
    """

    # Apply rolling median calculation for each column in y_axis
    for col_name in y_axis:
        df = df.withColumn(f"rolling_median_{col_name}", F.expr(f"percentile_approx({col_name}, 0.5) OVER (partition By {x_axis} ORDER BY {x_axis} RANGE BETWEEN {window_size*60*60} PRECEDING AND CURRENT ROW)"))

    return df


def build_daily_aggregate_view(df):
    """
    This function builds a daily aggregate view of the input DataFrame.
    :param df: A PySpark DataFrame
    :return: DataFrame with daily aggregate view
    """

    # Group by DeliveryStartDate and aggregate max SpnGeneration and sum of Total_ExecutedVolume
    df_daily_aggregate = df.groupBy("DeliveryStartDate").agg(
        F.max("max_SpnGeneration").alias("max_SpnGeneration"),
        F.sum("Total_ExecutedVolume").alias("sum_Total_ExecutedVolume")
    )

    return df_daily_aggregate


def build_weekly_aggregate_view(df):
    """
    This function builds a weekly aggregate view of the input DataFrame.
    :param df: A PySpark DataFrame
    :return: DataFrame with weekly aggregate view
    """

    # Group by week of year and aggregate mean of max SpnGeneration and sum of Total_ExecutedVolume
    df_weekly_aggregate = df.groupBy(F.weekofyear("DeliveryStartDate").alias("WeekOfYear")).agg(
        F.mean("max_SpnGeneration").alias("mean_max_SpnGeneration"),
        F.sum("Total_ExecutedVolume").alias("sum_Total_ExecutedVolume")
    )

    return df_weekly_aggregate
