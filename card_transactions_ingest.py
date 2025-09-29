# Sample PySpark notebook for ingesting card transactions (card_transactions_ingest.py)
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
import yaml, sys, os

spark = SparkSession.builder.appName("card_transactions_ingest").getOrCreate()

# Load config (YAML)
cfg_path = os.environ.get("CONFIG_PATH", "configs/ingest_config.yml")
try:
    import yaml
    with open(cfg_path, 'r') as f:
        cfg = yaml.safe_load(f)
except Exception as e:
    cfg = {}

# Example: read raw parquet/csv from landing
landing_path = cfg.get('landing_path', '/mnt/datalake/landing/card_txns/')
df = spark.read.option('header', True).csv(landing_path).limit(1000)

# Basic transformations
df = df.withColumn("ingest_ts", current_timestamp())
df = df.withColumnRenamed("txn_id", "transaction_id")

# Example fraud rule: flag high amount
df = df.withColumn("fraud_flag_high_amount", (col("amount").cast("double") > 10000).cast("int"))

# Write to curated path (delta)
curated_path = cfg.get('curated_path', '/mnt/datalake/curated/card_txns/')
try:
    df.write.mode('append').parquet(curated_path)
except Exception as e:
    print("Write error:", e)

print("Ingest completed - rows:", df.count())
