from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)

# The Glue job container runs on the same Docker network as the Snowflake
# emulator, so it reaches it by service name over plain HTTP.
sfOptions = {
    "sfURL": "snowflake:4566",
    "sfSSL": "off",
    "sfAccount": "test",
    "sfUser": "test",
    "sfPassword": "test",
    "sfDatabase": "test",
    "sfSchema": "public",
    "sfWarehouse": "test",
    "application": "AWSGlue",
}

# Extract: read the table created by init.sf.sql
df = spark.read \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("query", "SELECT * FROM src_glue") \
    .load()
df.show()
logger.info(f"Read {df.count()} rows from src_glue")

# Transform: keep the active rows
active = df.filter(df["STATUS"])

# Load: write them to a new Snowflake table
active.write \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbtable", "tgt_glue") \
    .mode("overwrite") \
    .save()
logger.info(f"Wrote {active.count()} rows to tgt_glue")

job.commit()
