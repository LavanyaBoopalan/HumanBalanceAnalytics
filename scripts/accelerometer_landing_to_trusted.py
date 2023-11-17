import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1700001577365 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1700001577365",
)

# Script generated for node customer_trusted
customer_trusted_node1700002622320 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1700002622320",
)

# Script generated for node SQL Query
SqlQuery301 = """
select acc.user, acc.timestamp, acc.x, acc.y, acc.z from acc JOIN cus ON acc.user = cus.email
"""
SQLQuery_node1700123156894 = sparkSqlQuery(
    glueContext,
    query=SqlQuery301,
    mapping={
        "acc": accelerometer_landing_node1700001577365,
        "cus": customer_trusted_node1700002622320,
    },
    transformation_ctx="SQLQuery_node1700123156894",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1700003274288 = glueContext.getSink(
    path="s3://stedihb-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1700003274288",
)
accelerometer_trusted_node1700003274288.setCatalogInfo(
    catalogDatabase="stedihb", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1700003274288.setFormat("json")
accelerometer_trusted_node1700003274288.writeFrame(SQLQuery_node1700123156894)
job.commit()
