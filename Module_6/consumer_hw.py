import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()


green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

# def peek(mini_batch, batch_id):
#     first_row = mini_batch.take(1)

#     if first_row:
#         print(first_row[0])

# query = green_stream.writeStream.foreachBatch(peek).start()

# query.stop()

# Question 6 : 

# green_stream = green_stream \
#   .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
#   .select("data.*")

# Print the records to the console
# query = green_stream \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# Wait for the termination of the query
# query.awaitTermination()

# Result of Question 6: 
# +--------------------+---------------------+------------+------------+---------------+-------------+----------+
# |lpep_pickup_datetime|lpep_dropoff_datetime|PULocationID|DOLocationID|passenger_count|trip_distance|tip_amount|
# +--------------------+---------------------+------------+------------+---------------+-------------+----------+
# | 2019-10-01 00:26:02|  2019-10-01 00:39:58|         112|         196|            1.0|         5.88|       0.0|
# | 2019-10-01 00:18:11|  2019-10-01 00:22:38|          43|         263|            1.0|          0.8|       0.0|
# | 2019-10-01 00:09:31|  2019-10-01 00:24:47|         255|         228|            2.0|          7.5|       0.0|
# | 2019-10-01 00:37:40|  2019-10-01 00:41:49|         181|         181|            1.0|          0.9|       0.0|
# | 2019-10-01 00:08:13|  2019-10-01 00:17:56|          97|         188|            1.0|         2.52|      2.26|
# | 2019-10-01 00:35:01|  2019-10-01 00:43:40|          65|          49|            1.0|         1.47|      1.86|
# | 2019-10-01 00:28:09|  2019-10-01 00:30:49|           7|         179|            1.0|          0.6|       1.0|
# | 2019-10-01 00:28:26|  2019-10-01 00:32:01|          41|          74|            1.0|         0.56|       0.0|
# | 2019-10-01 00:14:01|  2019-10-01 00:26:16|         255|          49|            1.0|         2.42|       0.0|
# | 2019-10-01 00:03:03|  2019-10-01 00:17:13|         130|         131|            1.0|          3.4|      2.85|
# | 2019-10-01 00:07:10|  2019-10-01 00:23:38|          24|          74|            3.0|         3.18|       0.0|
# | 2019-10-01 00:25:48|  2019-10-01 00:49:52|         255|         188|            1.0|          4.7|       1.0|
# | 2019-10-01 00:03:12|  2019-10-01 00:14:43|         129|         160|            1.0|          3.1|       0.0|
# | 2019-10-01 00:44:56|  2019-10-01 00:51:06|          18|         169|            1.0|         1.19|      0.25|
# | 2019-10-01 00:55:14|  2019-10-01 01:00:49|         223|           7|            1.0|         1.09|      1.46|
# | 2019-10-01 00:06:06|  2019-10-01 00:11:05|          75|         262|            1.0|         1.24|      2.01|
# | 2019-10-01 00:00:19|  2019-10-01 00:14:32|          97|         228|            1.0|         3.03|      3.58|
# | 2019-10-01 00:09:31|  2019-10-01 00:20:41|          41|          74|            1.0|         2.03|      2.16|
# | 2019-10-01 00:30:36|  2019-10-01 00:34:30|          41|          42|            1.0|         0.73|      1.26|
# | 2019-10-01 00:58:32|  2019-10-01 01:05:08|          41|         116|            1.0|         1.48|       0.0|
# +--------------------+---------------------+------------+------------+---------------+-------------+----------+


# Quetion 7 : 

popular_destinations = green_stream \
    .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", F.current_timestamp()) \
    .groupBy(F.window("timestamp", "5 minutes"), "DOLocationID") \
    .count() \
    .orderBy(F.col("count").desc())


query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()