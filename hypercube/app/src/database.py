
from pyspark.sql import DataFrameWriter

def write_to_mysql(df, table_name):
    """
    Write DataFrame to MySQL database.
    :param df: PySpark DataFrame to be written to the database
    :param table_name: Name of the table to write to
    :param url: JDBC URL of the MySQL database
    """
    
    df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/wind_db") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", table_name) \
    .option("user", "Sathya") \
    .option("password", "Sathya@123") \
    .option("mode", "append") \
    .option("batchsize", 1000) \
    .save()
