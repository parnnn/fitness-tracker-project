import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3
import os

# Specify your S3 bucket and path
bucket_name = 'fitnesstracker-staging-264384440796-ap-southeast-1-an'
prefix = 'warehouse/'

# Initialize S3 client
s3 = boto3.client('s3')


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    # Check if any of the frames in the mapping is empty
    if any(frame.count() == 0 for alias, frame in mapping.items()):
        # If any frame is empty, return the non-empty frame
        non_empty_frame = next(frame for alias, frame in mapping.items() if frame.count() > 0)
        return non_empty_frame
    else:
        # All frames are non-empty, perform the union
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(
            "(select * from {}) UNION {} (select * from {})".format(*mapping.keys())
        )
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script  for node Workout Log
Workout_log_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://fitnesstracker-staging-264384440796-ap-southeast-1-an/staging/workout_log/"],
        "recurse": True,
    },
    transformation_ctx="Workout_log_node",
)
print("Read Workout Log Data")


# Script for node Users
Users_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://fitnesstracker-staging-264384440796-ap-southeast-1-an/staging/users/"],
        "recurse": True,
    },
    transformation_ctx="Users_node",
)
print("Read Users Data")

# Script for node DW
DW_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://fitnesstracker-staging-264384440796-ap-southeast-1-an/warehouse/"],
        "recurse": True,
    },
    transformation_ctx="DW_node",
)
print("Read DW Data")

# Script  for node Join
Join_node = Join.apply(
    frame1=Workout_log_node,
    frame2=Users_node,
    keys1=["user_id"],
    keys2=["user_id"],
    transformation_ctx="Join_node",
)
print("Join Sucessful")

# Script  for node Union
Union_node = sparkUnion(
    glueContext,
    unionType="ALL",
    mapping={"source1": Join_node, "source2": DW_node},
    transformation_ctx="Union_node",
)

print("Union Sucessful")

# List all objects in the specified S3 path
objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix)['Contents']

# Delete each object
for obj in objects:
    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
print("Delete existing files") 
    

AmazonS3_node = glueContext.write_dynamic_frame.from_options(
    frame=Union_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://fitnesstracker-staging-264384440796-ap-southeast-1-an/warehouse/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node",
)
print("Save the data")
job.commit()