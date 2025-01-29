import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import
from py4j.protocol import Py4JJavaError

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)
## uj = sc._jvm.net.snowflake.spark.snowflake
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions = {
    "sfURL" : "https://dummy.snowflakecomputing.com",
    "sfUser" : "dummy_user",
    "sfPassword" : "dummy_password",
    "sfDatabase" : "dummy_db",
    "sfSchema" : "dummy_schema",
    "sfWarehouse" : "dummy_warehouse",
    "application" : "AWSGlue"
}

## Read from a Snowflake table into a Spark Data Frame
try:
    df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "dummy_table").load()
except Py4JJavaError as e:
    if ("JDBC driver encountered communication error" in str(e)):
        logger.info("JDBC_COMMUNICATION_ERROR")