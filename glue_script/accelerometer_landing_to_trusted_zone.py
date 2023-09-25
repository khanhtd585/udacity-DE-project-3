import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1695610422063 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1695610422063",
)

# Script generated for node customer_trusted
customer_trusted_node1695610256714 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-lh/customer/trusted/"], "recurse": True},
    transformation_ctx="customer_trusted_node1695610256714",
)

# Script generated for node Join
Join_node1695610311264 = Join.apply(
    frame1=customer_trusted_node1695610256714,
    frame2=accelerometer_landing_node1695610422063,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1695610311264",
)

# Script generated for node Drop Fields
DropFields_node1695611339705 = DropFields.apply(
    frame=Join_node1695610311264,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "timestamp",
    ],
    transformation_ctx="DropFields_node1695611339705",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1695610595140 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695611339705,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lh/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1695610595140",
)

job.commit()
