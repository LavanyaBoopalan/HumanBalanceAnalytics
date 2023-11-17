import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
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

# Script generated for node customer_trusted
customer_trusted_node1700004014418 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1700004014418",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1700004054432 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedihb-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1700004054432",
)

# Script generated for node SQL Query
SqlQuery288 = """
SELECT
cus.customername,
cus.email,
cus.phone,
cus.birthday,
cus.serialnumber,
cus.registrationdate,
cus.lastupdatedate,
cus.sharewithresearchasofdate,
cus.sharewithpublicasofdate,
cus.sharewithfriendsasofdate
FROM cus LEFT SEMI JOIN acc
ON cus.email = acc.user
"""
SQLQuery_node1700124201350 = sparkSqlQuery(
    glueContext,
    query=SqlQuery288,
    mapping={
        "cus": customer_trusted_node1700004014418,
        "acc": accelerometer_trusted_node1700004054432,
    },
    transformation_ctx="SQLQuery_node1700124201350",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    SQLQuery_node1700124201350,
    ["Name", "EMAIL", "phone", "PERSON_NAME", "PHONE_NUMBER"],
    1.0,
    0.1,
)


def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("***"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")


DetectSensitiveData_node1700116344048 = maskDf(
    SQLQuery_node1700124201350, list(classified_map.keys())
)

# Script generated for node customer_curated
customer_curated_node1700004416538 = glueContext.getSink(
    path="s3://stedihb-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1700004416538",
)
customer_curated_node1700004416538.setCatalogInfo(
    catalogDatabase="stedihb", catalogTableName="customer_curated"
)
customer_curated_node1700004416538.setFormat("json")
customer_curated_node1700004416538.writeFrame(DetectSensitiveData_node1700116344048)
job.commit()
