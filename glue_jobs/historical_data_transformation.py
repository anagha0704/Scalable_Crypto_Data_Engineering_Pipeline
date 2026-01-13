import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, explode, expr, from_unixtime, from_utc_timestamp
import boto3
import botocore
from urllib.parse import urlparse
from awsglue.dynamicframe import DynamicFrame

def s3_path_exists(s3_uri):
    """
    Checks if the given S3 path exists by attempting to list objects at the prefix.
    Returns True if objects exist, False otherwise.
    """
    s3 = boto3.client('s3')
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')
    try:
        result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return 'Contents' in result
    except botocore.exceptions.ClientError as e:
        return False
        
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load historical data from Glue Catalog as a DynamicFrame
dyf_hist = glueContext.create_dynamic_frame.from_catalog(
    database="crypto_raw_db",
    table_name="historical_data"
)

# Convert DynamicFrame to Spark DataFrame
df_hist = dyf_hist.toDF()

# Explode prices array into separate rows
hist_prices = df_hist.withColumn("price_row", explode(col("data.prices"))).select(
    col("coin_id"),
    col("coin_name"),
    col("coin_symbol"),
    col("coin_image"),
    col("market_cap_rank"),
    expr("price_row[0].long").alias("timestamp"),
    expr("price_row[1].double").alias("price")
)

# Explode market_caps array into separate rows
hist_mc = df_hist.withColumn("mc_row", explode(col("data.market_caps"))).select(
    col("coin_id"),
    expr("mc_row[0].long").alias("timestamp"),
    expr("mc_row[1].double").alias("market_cap")
)

# Explode total_volumes array into separate rows
hist_volume = df_hist.withColumn("vol_row", explode(col("data.total_volumes"))).select(
    col("coin_id"),
    expr("vol_row[0].long").alias("timestamp"),
    expr("vol_row[1].double").alias("volume")
)

# Join prices, market_caps, and volumes on coin_id and timestamp
incoming_df = hist_prices.join(hist_mc, ["coin_id", "timestamp"]) \
                         .join(hist_volume, ["coin_id", "timestamp"]) \
                         .withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp") / 1000), "America/Chicago"))

s3_path = "s3://crypto-transformed-data-0704/historical_data/"

# Check if S3 path exists and load existing data if present
if s3_path_exists(s3_path):
    existing_df = spark.read.parquet(s3_path)
    print("Existing data loaded from S3.")
else:
    existing_df = None
    print("No existing data found. Skipping read.")

# Combine new and existing data, dropping duplicates by coin_id and timestamp
if existing_df is not None:
    combined_df = incoming_df.unionByName(existing_df).dropDuplicates(["coin_id", "timestamp"])
else:
    combined_df = incoming_df
                         
combined_df.write.mode("overwrite").parquet("s3://crypto-transformed-data-0704/historical_data/")

job.commit()