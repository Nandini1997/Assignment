import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

from Assignment1_methods import to_timestamp,to_snake_case,to_milliseconds

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Assignment").getOrCreate()

    # Ingesting the input data to perform transformation
    df = spark.read \
        .option("header", True) \
        .csv("C:\\Users\\NandiniSrinivas\\PycharmProjects\\pyspark_assignment\\Resource\\input.csv")

    # converting milliseconds to time stamp format
    df_with_timestamp = to_timestamp(df, "Issue_Date_timestamp")

    # Converting Datetime to Date format
    df_to_date = df_with_timestamp.withColumn("Issue_Date_Format", to_date("Issue_Date_timestamp"))

    # Fill null with nothing
    product_details_res = df_to_date.na.fill("").select(col("ProductName")
                                                        , col("Price"), col("Brand"), col("Country"),
                                                        col("Product number").alias("product_number"), col("Issue_Date_Format"))

    # Ingesting the input data to perform transformation
    source_df = spark.read \
        .option("header", True) \
        .csv("C:\\Users\\NandiniSrinivas\\PycharmProjects\\pyspark_assignment\\Resource\\Source.csv")

    # Converting camel case to snake case
    df_2 = to_snake_case(source_df)

    # convert timestamp to milliseconds
    df_epoch = to_milliseconds(df_2, "start_time_ms")

    df_epoch = df_2.withColumn("start_time_ms",
                               (F.col("start_time").cast("timestamp").cast("double") * 1000).cast("long"))

    df_result = df_epoch.join(product_details_res, df_epoch.product_number == product_details_res.product_number, "inner").filter(
        df_epoch.language == 'EN')

    df_result.show()