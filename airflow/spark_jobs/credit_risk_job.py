import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


table = sys.argv[1]
s3_path = sys.argv[2]


spark = (
    SparkSession.builder
    .appName("CreditRiskProcessing")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .getOrCreate()
)

jdbc_url = (
    f"jdbc:postgresql://{os.environ['PG_HOST']}:"
    f"{os.environ['PG_PORT']}/{os.environ['PG_DATABASE']}"
)

properties = {
    "user": os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
    "driver": "org.postgresql.Driver"
}


applications_df = spark.read.jdbc(
    url=jdbc_url,
    table=table,
    properties=properties
)


applications_df.show(10, truncate=False)

print("Total applications:", applications_df.count())


blacklist_df = spark.read.csv(
    "/opt/airflow/data/blacklist.csv",
    header=True
)

blacklist_df.show()

print("Blacklist records:", blacklist_df.count())


applications_df = applications_df.withColumn(
    "customer_id",
    F.trim(F.col("customer_id"))
)

blacklist_df = blacklist_df.withColumn(
    "customer_id",
    F.trim(F.col("customer_id"))
)

joined_df = applications_df.join(
    blacklist_df,
    on="customer_id",
    how="left"
)

joined_df.select(
    "customer_id",
    "customer_name",
    "reason"
).show(10, truncate=False)

print("Joined rows:", joined_df.count())

joined_df.filter(
    F.col("reason").isNotNull()
).select(
    "customer_id", "reason"
).show()


result_df = joined_df.withColumn(
    "risk_status",
    F.when(
        F.col("reason").isNotNull(),
        "rejected"
    )
    .when(
        (F.col("requested_amount") > F.col("monthly_income") * 12)
        | (F.col("existing_debt") > F.col("monthly_income") * 0.7)
        | (F.col("employment_years") < 1)
        | (F.col("age") < 21),
        "high_risk"
    )
    .otherwise("approved")
)

result_df.select(
    "customer_id",
    "customer_name",
    "risk_status"
).show(10, truncate=False)

result_df.groupBy("risk_status").count().show()

result_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("application_date") \
    .save(s3_path)

print(f"Delta Lake data saved to: {s3_path}")

delta_df = (
    spark.read
    .format("delta")
    .load(s3_path)
)

delta_df.select(
    "customer_id",
    "customer_name",
    "application_date",
    "risk_status"
).show(10, truncate=False)

print("Delta records:", delta_df.count())

delta_df.groupBy("risk_status").count().show()


spark.stop()