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

# Script generated for node customer curated
customercurated_node1695624603029 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1695624603029",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1695623020469 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lh/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1695623020469",
)

# Script generated for node Join
Join_node1695624619123 = Join.apply(
    frame1=customercurated_node1695624603029,
    frame2=step_trainer_trusted_node1695623020469,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1695624619123",
)

# Script generated for node Drop Fields
DropFields_node1695625411417 = DropFields.apply(
    frame=Join_node1695624619123,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1695625411417",
)

# Script generated for node Step Trainer Curated
StepTrainerCurated_node1695610595140 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695625411417,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lh/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerCurated_node1695610595140",
)

job.commit()
