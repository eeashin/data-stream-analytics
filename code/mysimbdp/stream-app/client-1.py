from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

checkpoint_battery_alert = "/tmp/spark_checkpoint/battery_alert_1"
checkpoint_query_df = "/tmp/spark_checkpoint/query_df_1"
checkpoint_query_analytics = "/tmp/spark_checkpoint/query_analytics_1"

# Create a Spark Session
spark = SparkSession.builder \
    .appName("streamAnalytics-2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.shuffle.service.enabled", "true") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.dynamicAllocation.initialExecutors", "1") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "4") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.mongodb.output.uri", "mongodb://admin:admin@127.0.0.1:30000/admin?authSource=admin") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "2")
# Define global variable for termination condition
terminate_query = False

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

# Define the schema for the DataFrame
schema = StructType([
    StructField("time", LongType()),
    StructField("readable_time", TimestampType()),
    StructField("acceleration", DoubleType()),  
    StructField("acceleration_x", IntegerType()),
    StructField("acceleration_y", IntegerType()),
    StructField("acceleration_z", IntegerType()),
    StructField("battery", IntegerType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("dev-id", StringType())
])

# Create a DataFrame that reads from the input Kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stream-topic-1")
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING) as kafka_value")
    .select(from_json(col("kafka_value"), schema).alias("data"))
    .select("data.*")
)
# filter to check if battery value is lower than 2000
low_battery_df = df.filter(col("battery") < 2000)

# Send dev-id and battery value to Kafka topic
battery_alert_query = (
    low_battery_df.selectExpr("CAST(`dev-id` AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "battery-alert-topic")
    .option("checkpointLocation", checkpoint_battery_alert)
    .start()
)

# Apply filtering and windowing within 1-minute intervals, then calculate average temperature and acceleration
analytics = (
    df.filter((col("temperature") > 20) & (col("humidity").between(30, 40)))
    .withWatermark("readable_time", "1 minute")
    .groupBy(window("readable_time", "1 minute"), "dev-id")
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("acceleration").alias("avg_acceleration"),
        count("*").alias("total_records")
    )
    .withColumn("throughput", col("total_records") / 60)
)

# Write the DataFrames near-real-time to MongoDB
query_df = (
    df.writeStream
    .outputMode("append")
    .foreachBatch(lambda df, epoch_id: df.write.format("mongo").mode("append").option("database", "stream_zoo").option("collection", "tortoise").save())
    .trigger(processingTime="1 second")
    .option("checkpointLocation", checkpoint_query_df)
    .start()
)

query_analytics = (
    analytics.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, epoch_id: df.write.format("mongo").mode("append").option("database", "stream_zoo").option("collection", "zoo_analytics").save())
    .trigger(processingTime="60 second")
    .option("checkpointLocation", checkpoint_query_analytics)
    .start()
)

try:
    while not terminate_query:
        # Run indefinitely until termination condition is set
        pass
except KeyboardInterrupt:
    # Set termination condition when KeyboardInterrupt is received
    terminate_query = True
    query_df.stop()
    query_analytics.stop()
    battery_alert_query.stop()