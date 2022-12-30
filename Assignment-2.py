from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count, trim, hour, to_timestamp

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Assignment").getOrCreate()

    df = spark.read.csv("C:\\Users\\NandiniSrinivas\\PycharmProjects\\pyspark_assignment\\Resource\\ghtorrent-logs.txt")
    torrent_log_df = df.withColumnRenamed("_c0", "LogLevel").withColumnRenamed("_c1", "timestamp").withColumnRenamed(
        "_c2", "ghtorrent_details")

    # Df with required fields
    torrent_cliendId_extract = torrent_log_df \
        .withColumn("ghtorrent_client_id", split(col("ghtorrent_details"), "--").getItem(0)) \
        .withColumn("repository", split(col("ghtorrent_details"), "--").getItem(1)) \
        .withColumn("downloader_id", split(col("ghtorrent_client_id"), "-").getItem(1)) \
        .withColumn("repository_torrent", split(col("repository"), ':').getItem(0)) \
        .withColumn("Request_status_ext", split(col("repository"), ':').getItem(1)) \
        .withColumn("Request_status", split(col("Request_status_ext"), ",").getItem(0)) \
        .drop(col("repository")) \
        .withColumn("request_url", split(col("ghtorrent_details"), "URL:").getItem(1)) \
        .drop(col("ghtorrent_details"))

    # total number of lines
    total_number_of_lines = torrent_cliendId_extract.select(count("*").alias("total_count"))
    total_number_of_lines.show()

    # total number of warnings
    warning_count = torrent_cliendId_extract.filter(col("LogLevel") == 'WARN').select(count("*").alias("warn_count"))
    warning_count.show()

    # How many repositories where processed in total? Use the api_client lines only.
    api_client_repo_count = torrent_cliendId_extract.filter(trim(col("repository_torrent")) == 'api_client.rb').select(
        count("*").alias("api_client_repo_count"))
    api_client_repo_count.show()

    # Which client did most HTTP requests?
    total_http_client_req = torrent_cliendId_extract.select(col("ghtorrent_client_id"),
                                                            col("request_url").isNotNull()).filter(
        col("(request_url IS NOT NULL)") == True).groupBy("ghtorrent_client_id").count()
    max_req_client = total_http_client_req.withColumnRenamed("ghtorrent_client_id", "max_req_client")
    client_max_req = max_req_client.sort(col("count").desc())
    client_max_req.show(1)

    # Which client did most FAILED HTTP requests?
    failed_count = torrent_cliendId_extract.filter(col("Request_status_ext").like("%Failed%")).groupBy(
        "ghtorrent_client_id").count()
    max_failed_client_req = failed_count.withColumnRenamed("ghtorrent_client_id", "max_failed_req_client")
    max_failed_client_red = max_failed_client_req.sort(col("count").desc())
    max_failed_client_red.show(1)

    # What is the most active hour of day?
    df_with_hours = torrent_cliendId_extract.withColumn("timestamp_new", to_timestamp(col("timestamp"))) \
        .withColumn("hours", hour(col("timestamp_new"))).groupBy("hours").count()
    df_active_hours = df_with_hours.withColumnRenamed("hours", "active_hours")
    df_active_hours.sort(col("count").desc()).show(1)

    # What is the most active repository
    count_repo = torrent_cliendId_extract.groupBy("repository_torrent").count()
    count_repo_active = count_repo.withColumnRenamed("repository_torrent", "active_repo_used")
    active_repository = count_repo_active.sort(col("count").desc()).show(1)
