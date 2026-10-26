import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MinIOConnectionTest")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .getOrCreate()
)

try:
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI("s3a://credit/"),
        spark._jsc.hadoopConfiguration()
    )

    print("MINIO CONNECTION OK")
    print("BUCKET:", fs.getUri())
finally:
    spark.stop()