
import sys
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME",  "clean_bucket", "final_bucket", "project"])
clean_bucket = args["clean_bucket"]
final_bucket = args["final_bucket"]
project = args["project"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_path = f"s3://{clean_bucket}/{project}/etl-jobs/"
output_path = f"s3://{final_bucket}/{project}/"

df = spark.read.option("header", True).csv(input_path)
if df.rdd.isEmpty():
    raise Exception(f"No data found at {input_path}")

df2 = df.withColumn("processed_at_utc", F.current_timestamp())
(df2.write
    .mode("overwrite")
    .parquet(output_path))

job.commit()
