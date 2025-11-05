
import sys
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "landing_bucket", "clean_bucket"])
landing_bucket = args["landing_bucket"]
clean_bucket = args["clean_bucket"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_path = f"s3://{landing_bucket}/etl-jobs/"
output_path = f"s3://{clean_bucket}/etl-jobs/"

df = spark.read.option("header", True).csv(input_path)
if df.rdd.isEmpty():
    raise Exception(f"No data found at {input_path}")

df2 = df.withColumn("processed_at_utc", F.current_timestamp())
(df2.write
    .mode("overwrite")
    .parquet(output_path))

job.commit()
