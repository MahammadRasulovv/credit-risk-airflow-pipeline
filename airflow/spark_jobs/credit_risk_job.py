
import os
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, trim, upper


# DAG-dan gələn parametrlər
parser = argparse.ArgumentParser()
parser.add_argument("--table", required=True)
parser.add_argument("--s3-path", required=True)
args = parser.parse_args()


# PostgreSQL və MinIO məlumatları
pg_user = os.environ["PG_USER"]
pg_password = os.environ["PG_PASSWORD"]

minio_user = os.environ["MINIO_ROOT_USER"]
minio_password = os.environ["MINIO_ROOT_PASSWORD"]


# SparkSession
spark = (
    SparkSession.builder
    .appName("CreditRiskPipeline")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    # MinIO konfiqurasiyası
    hconf = spark.sparkContext._jsc.hadoopConfiguration()

    hconf.set("fs.s3a.endpoint", "http://minio:9000")
    hconf.set("fs.s3a.access.key", minio_user)
    hconf.set("fs.s3a.secret.key", minio_password)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    hconf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    # PostgreSQL-dən məlumatların oxunması
    df = (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/airflow")
        .option("dbtable", args.table)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("POSTGRES ROW COUNT:", df.count())

    # customer_id təmizlənməsi
    df = df.withColumn(
        "customer_id",
        upper(trim(col("customer_id")))
    )

    # Blacklist CSV oxunması
    blacklist = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv("/opt/airflow/data/blacklist.csv")
        .select(
            upper(trim(col("customer_id"))).alias("customer_id")
        )
        .distinct()
        .withColumn("is_blacklisted", lit(True))
    )

    # Blacklist yoxlanması
    print("BLACKLIST DATA:")
    blacklist.show(truncate=False)

    # LEFT JOIN
    joined = df.join(
        blacklist,
        on="customer_id",
        how="left"
    )

    print("BLACKLIST MATCHES:")
    joined.filter(
        col("is_blacklisted") == True
    ).select(
        "customer_id",
        "is_blacklisted"
    ).show(truncate=False)

    # Risk statusunun hesablanması
    result = (
        joined
        .withColumn(
            "risk_status",
            when(
                col("is_blacklisted") == True,
                "rejected"
            )
            .when(
                (col("requested_amount") > col("monthly_income") * 12)
                | (col("existing_debt") > col("monthly_income") * 0.7)
                | (col("employment_years") < 1)
                | (col("age") < 21),
                "high_risk"
            )
            .otherwise("approved")
        )
        .drop("is_blacklisted")
    )

    # Risk nəticələri
    print("RISK STATUS RESULTS:")

    status_counts = (
        result
        .groupBy("risk_status")
        .count()
    )

    status_counts.show(truncate=False)

    # Nəticənin yoxlanması
    actual_counts = {
        row["risk_status"]: row["count"]
        for row in status_counts.collect()
    }

    expected_counts = {
        "approved": 4,
        "high_risk": 4,
        "rejected": 2
    }

    if actual_counts != expected_counts:
        raise ValueError(
            f"Risk status nəticəsi yanlışdı: {actual_counts}"
        )

    # Delta formatında MinIO-ya yazma
    (
        result.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("application_date")
        .save(args.s3_path)
    )

    print("DELTA WRITE SUCCESS:", args.s3_path)

finally:
    spark.stop()
