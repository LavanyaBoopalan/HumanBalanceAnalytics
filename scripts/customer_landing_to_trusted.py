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

# Script generated for node customer_landing
customer_landing_node1699973168707 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1699973168707",
)

# Script generated for node Share_With_Research
SqlQuery749 = """
select * from cust where sharewithresearchasofdate != 0
"""
Share_With_Research_node1699975790759 = sparkSqlQuery(
    glueContext,
    query=SqlQuery749,
    mapping={"cust": customer_landing_node1699973168707},
    transformation_ctx="Share_With_Research_node1699975790759",
)

# Script generated for node customer_trusted
customer_trusted_node1699973369155 = glueContext.getSink(
    path="s3://stedihb-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1699973369155",
)
customer_trusted_node1699973369155.setCatalogInfo(
    catalogDatabase="stedihb", catalogTableName="customer_trusted"
)
customer_trusted_node1699973369155.setFormat("json")
customer_trusted_node1699973369155.writeFrame(Share_With_Research_node1699975790759)
job.commit()
