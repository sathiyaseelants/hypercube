#!/usr/bin/env python3

import os.path
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import functions as F
from pyspark.ml.feature import Imputer

from pyspark.sql.functions import col, round, explode, to_date, count, when, weekofyear

def get_numeric_columns(input_df):
    return [col_name for col_name, data_type in input_df.dtypes if data_type in ['int', 'bigint', 'float', 'double', 'decimal', 'tinyint', 'smallint']]


def drop_null_columns(df):
  
    """
    This function drops columns containing all null values.
    :param df: A PySpark DataFrame
    """
    
    _df_length = df.count()
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    print(null_counts)
    to_drop = [k for k, v in null_counts.items() if v == _df_length]
    df = df.drop(*to_drop)
    
    return df


def clean_data(df):
      
    """
    This function cleans the data by:
        1. Checks for Empty DataFrame
        2. Replaces NULL to None
        3. Drops columns with all null/none values
        4. Drops duplicate rows
        5. Finds numerical columns and impute null values them Mean
        6. Rounds Imputed column values to 2 decimal places
    :param df: A PySpark DataFrame
    """
    
    # Handle enpty DataFrame
    if df.isEmpty():
        return None

    # Replace NULL to None for transformations without errors
    # We can also convert NaN to None, NULL to NaN, etc depends upon the usecase and ML library requirement
    df = df.replace('None', None).replace('NULL', None).replace('NaN', None).replace(float('NaN'), None)
    #df = df.fillna(None).fillna(float('nan'), None)

    # Drop columns with all null values
    #df = drop_null_columns(df)

    # Drop duplicates
    df = df.dropDuplicates()

    # Apply imputer to numerical columns
    numerical_cols = get_numeric_columns(df)

    imputer = Imputer(
        inputCols=numerical_cols,
        outputCols=["{}_imputed".format(c) for c in numerical_cols]
    ).setStrategy("mean")
    df_imputed = imputer.fit(df).transform(df)

    # Round imputed numerical columns to 2 decimal places
    numerical_cols_imputed = get_numeric_columns(df_imputed)
    for col_name in numerical_cols_imputed:
        if col_name.endswith('_imputed'):
            df_imputed = df_imputed.withColumn(col_name, F.round(F.col(col_name), 2))
    
    return df_imputed
