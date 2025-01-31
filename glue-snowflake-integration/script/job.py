from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions = {
    "sfURL" : "https://snowflake.localhost.localstack.cloud",
    "sfUser" : "test",
    "sfPassword" : "test",
    "sfDatabase" : "test",
    "sfSchema" : "public",
    "sfWarehouse" : "test",
    "application" : "AWSGlue"
}

# Read from a Snowflake table into a Spark Data Frame
try:
    # Execute select query from table created using init script
    query = "SELECT * FROM src_glue"
    df = spark.read \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("query", query) \
        .load()

    logger.info("Query executed successfully")

    # Show the results
    df.show()

    job.commit()
except Exception as e:
    if ("JDBC driver encountered communication error" in str(e)):
        logger.info("JDBC_COMMUNICATION_ERROR")
