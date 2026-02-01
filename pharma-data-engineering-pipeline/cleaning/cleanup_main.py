from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date
from configs.config import LANDING_PATHS, SILVER_PATHS, SCHEMA_METADATA

def apply_text_cleanup(df, column):
    return df.withColumn(column, lower(trim(col(column))))

def apply_date_cleanup(df, column):
    return df.withColumn(column, to_date(col(column)))

spark = SparkSession.builder.appName("CleanupMain").getOrCreate()

for table, schema in SCHEMA_METADATA.items():
    df = spark.read.parquet(LANDING_PATHS[table])

    for column, dtype in schema.items():
        if dtype == "string":
            df = apply_text_cleanup(df, column)

        if column.endswith("_date"):
            df = apply_date_cleanup(df, column)

    df.write.mode("overwrite").parquet(SILVER_PATHS[table])

spark.stop()
