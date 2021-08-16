from pyspark.sql import SparkSession
spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.20.15.23:9092") \
  .option("subscribe", "backend.emergency_notice_service.legacyEN") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "C:/Users/admin/PycharmProjects/spark1/data1.parquet") \
        .option('checkpointLocation', "C:/Users/admin/PycharmProjects/spark1/checkpoint/") \
        .start() \
        .awaitTermination()
