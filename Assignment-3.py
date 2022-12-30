from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,countDistinct, sum
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Assignment").getOrCreate()

    transactionSchema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("userid", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("product_description", StringType(), True)
    ])
    userSchema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("emailid", StringType(), True),
        StructField("nativelanguage", StringType(), True),
        StructField("location", StringType(), True)
    ])

    transaction_df = spark.read.option("header", True).schema(transactionSchema).csv("C:\\Users\\NandiniSrinivas\\PycharmProjects\\pyspark_assignment\\Resource\\transaction.csv")
    user_df = spark.read.option("header", True).schema(userSchema).csv("C:\\Users\\NandiniSrinivas\\PycharmProjects\\pyspark_assignment\\Resource\\user.csv")

    transaction_user_df = transaction_df.join(user_df, transaction_df.userid == user_df.user_id, "inner")

    count_of_location = transaction_user_df.select(countDistinct("location").alias("Distinct_location_count"))

    products_users = transaction_user_df.select(col("userid"), col("product_description"))

    spending_each_price = transaction_user_df.groupBy("userid", "product_id").agg(sum("price"))

    count_of_location.show()
    products_users.show()
    spending_each_price.show()