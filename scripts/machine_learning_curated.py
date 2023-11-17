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

# Script generated for node steptrainer_trusted
steptrainer_trusted_node1700068839771 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="steptrainer_trusted_node1700068839771",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1700068914428 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1700068914428",
)

# Script generated for node SQL Query
SqlQuery101 = """
SELECT st.sensorreadingtime, st.serialnumber, st.distancefromobject FROM st INNER JOIN acc ON st.sensorreadingtime = acc.timestamp
"""
SQLQuery_node1700068977465 = sparkSqlQuery(
    glueContext,
    query=SqlQuery101,
    mapping={
        "st": steptrainer_trusted_node1700068839771,
        "acc": accelerometer_trusted_node1700068914428,
    },
    transformation_ctx="SQLQuery_node1700068977465",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1700069291940 = glueContext.getSink(
    path="s3://stedihb-lake-house/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1700069291940",
)
machine_learning_curated_node1700069291940.setCatalogInfo(
    catalogDatabase="stedihb", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1700069291940.setFormat("json")
machine_learning_curated_node1700069291940.writeFrame(SQLQuery_node1700068977465)
job.commit()
