import pyspark.sql.functions as F
from functools import reduce
import re


def to_timestamp(df, column_to_add):
    df_ret = df.withColumn(column_to_add, (F.col("IssueDate") / 1000).cast("timestamp"))
    return df_ret


def to_snake_case(source_df):
    column_list = source_df.columns
    df_ret = reduce(lambda new_df, i: new_df.withColumnRenamed(i, re.sub(r'(?<!^)(?=[A-Z])', '_', i).lower()),
                    column_list, source_df)
    return df_ret


def to_milliseconds(df_2,column_name):
    df_ret=df_2.withColumn(column_name,(F.col("start_time").cast("timestamp").cast("double")*1000).cast("long"))
    return df_ret