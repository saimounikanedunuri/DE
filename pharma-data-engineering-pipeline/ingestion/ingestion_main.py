from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from configs.config import FILE_PATHS, DB_CONFIG, KAFKA_CONFIG
from utils.logging import get_logger

spark = SparkSession.builder.appName("IngestionMain").getOrCreate()

logger = get_logger(
    pipeline_name="ingestion_main",
    layer="ingestion"
)

logger.info("Pipeline started")

try:
    # =========================
    # File ingestion
    # =========================
    for name, path in FILE_PATHS.items():
        logger.info(f"Starting file ingestion | source={name} | path={path}")

        df = (
            spark.read
            .option("header", "true")
            .csv(path)
            .withColumn("source_type", lit("file"))
            .withColumn("source_name", lit(name))
            .withColumn("ingestion_ts", current_timestamp())
        )

        rows = df.count()
        logger.info(f"File read complete | source={name} | rows={rows}")

        df.write.mode("append").parquet(f"data/landing/files/{name}")
        logger.info(f"File write successful | target=data/landing/files/{name}")

    # =========================
    # Database ingestion
    # =========================
    for table in DB_CONFIG["tables"]:
        logger.info(f"Starting DB ingestion | table={table}")

        df = (
            spark.read
            .format("jdbc")
            .option("url", DB_CONFIG["url"])
            .option("dbtable", table)
            .option("user", DB_CONFIG["user"])        # secrets in real
            .option("password", DB_CONFIG["password"])
            .load()
            .withColumn("source_type", lit("database"))
            .withColumn("source_name", lit(table))
            .withColumn("ingestion_ts", current_timestamp())
        )

        rows = df.count()
        logger.info(f"DB read complete | table={table} | rows={rows}")

        df.write.mode("append").parquet(f"data/landing/database/{table}")
        logger.info(f"DB write successful | target=data/landing/database/{table}")

    # =========================
    # Kafka ingestion
    # =========================
    for topic in KAFKA_CONFIG["topics"]:
        logger.info(f"Starting Kafka ingestion | topic={topic}")

        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"])
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .withColumn("source_type", lit("kafka"))
            .withColumn("source_name", lit(topic))
            .withColumn("ingestion_ts", current_timestamp())
        )

        rows = df.count()
        logger.info(f"Kafka read complete | topic={topic} | rows={rows}")

        df.write.mode("append").parquet(f"data/landing/kafka/{topic}")
        logger.info(f"Kafka write successful | target=data/landing/kafka/{topic}")

    logger.info("Pipeline completed successfully")

except Exception as e:
    logger.error("Pipeline failed", exc_info=True)
    raise

finally:
    spark.stop()
    logger.info("Spark session stopped")
