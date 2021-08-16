

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col,from_json

spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

schema = ArrayType(StructType([
    StructField("dick",StringType(),True),
    StructField("vagina",StringType(),True)
  ]))

data = spark.read.parquet("C:/Users/admin/PycharmProjects/spark1/data.parquet")
dfJSON = data.select(col("id"), \
         explode(from_json(col("data"),schema)).alias("jsonData")) \
        .select("Id","jsonData.*")
dfJSON.show()

