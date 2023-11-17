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

# Script generated for node step_trainer_landing
step_trainer_landing_node1700003496510 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1700003496510",
)

# Script generated for node customer_curated
customer_curated_node1700068010403 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1700068010403",
)

# Script generated for node SQL Query
SqlQuery100 = """
SELECT st.sensorreadingtime,st.serialnumber,st.distancefromobject from cust INNER JOIN st ON cust.serialnumber = st.serialnumber
"""
SQLQuery_node1700068077025 = sparkSqlQuery(
    glueContext,
    query=SqlQuery100,
    mapping={
        "cust": customer_curated_node1700068010403,
        "st": step_trainer_landing_node1700003496510,
    },
    transformation_ctx="SQLQuery_node1700068077025",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1700068374572 = glueContext.getSink(
    path="s3://stedihb-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1700068374572",
)
step_trainer_trusted_node1700068374572.setCatalogInfo(
    catalogDatabase="stedihb", catalogTableName="steptrainer_trusted"
)
step_trainer_trusted_node1700068374572.setFormat("json")
step_trainer_trusted_node1700068374572.writeFrame(SQLQuery_node1700068077025)
job.commit()
