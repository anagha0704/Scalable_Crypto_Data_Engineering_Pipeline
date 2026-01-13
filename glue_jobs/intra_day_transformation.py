
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, explode, struct, expr, from_unixtime, from_utc_timestamp
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Load the data from the Glue catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="crypto_raw_db",
    table_name="intra_day_data"
)

# 2. Convert to Spark DataFrame
df = dyf.toDF()

# Explode prices structure
df_exploded_price = df.withColumn("price_row", explode(col("data.prices"))).select(
    col("coin_id"),
    col("coin_name"),
    col("coin_symbol"),
    col("coin_image"),
    col("market_cap_rank"),

    # Extract timestamp and values from each array of choice type
    expr("price_row[0].long").alias("timestamp"),
    expr("price_row[1].double").alias("price"))
    
# Explode market_caps structure
df_exploded_mc = df.withColumn("market_cap_row", explode(col("data.market_caps"))).select(
    col("coin_id"),
    # col("coin_name"),
    # col("coin_symbol"),
    # col("coin_image"),
    # col("market_cap_rank"),

    # Extract timestamp and values from each array of choice type
    expr("market_cap_row[0].long").alias("timestamp"),
    expr("market_cap_row[1].double").alias("market_cap"))
    
    
# Explode total_volumes structure
df_exploded_volume = df.withColumn("volume_row", explode(col("data.total_volumes"))).select(
    col("coin_id"),
    # col("coin_name"),
    # col("coin_symbol"),
    # col("coin_image"),
    # col("market_cap_rank"),

    # Extract timestamp and values from each array of choice type
    expr("volume_row[0].long").alias("timestamp"),
    expr("volume_row[1].double").alias("volume"))

# Joined all exploded dfs on timestamp and coin_id
joined_df = df_exploded_price.join(df_exploded_mc, ["coin_id", "timestamp"]) \
                     .join(df_exploded_volume, ["coin_id", "timestamp"])
                     
# Convert timestamp (in ms) to readable datetime format
joined_df = joined_df.withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp") / 1000), "America/Chicago"))
                     
# Write flattened data to S3
joined_df.write.mode("overwrite").parquet("s3://crypto-transformed-data-0704/intra_day/")

job.commit()