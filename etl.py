import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3


# Specify your S3 bucket and path for the data warehouse
bucket_name = 'fitnesstracker-staging-264384440796-ap-southeast-1-an'
prefix = 'warehouse/'

# Initialize S3 client for manual file deletion later
s3 = boto3.client('s3')


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    # Check if any of the frames in the mapping is empty
    if any(frame.count() == 0 for alias, frame in mapping.items()):
        # If any frame is empty, return the non-empty frame to avoid schema conflicts
        non_empty_frame = next(frame for alias, frame in mapping.items() if frame.count() > 0)
        return non_empty_frame
    else:
        # All frames are non-empty, perform with the union
        #Extract actual DataFrames from the mapping dictionary
        tables = list(mapping.values())
        df1 = tables[0].toDF()
        df2 = tables[1].toDF()

        result = df1.unionByName(df2, allowMissingColumns=True)

        #Drop dupliacates if unionType is not "ALL"
        if unionType != "ALL":
            result = result.distinct()

        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Initialize Glue Job and Spark contexts
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Extract: Read Workout Log Data from staging (Format:CSV)
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


# Extract: Read Users Data from staging (Format:CSV)
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

# Extract: Read Existing Data from Data Warehouse (Format:Parquet)
DW_node = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://fitnesstracker-staging-264384440796-ap-southeast-1-an/warehouse/"],
        "recurse": True,
    },
    transformation_ctx="DW_node",
)
print("Read DW Data")

# Transform: Join Workout Log and Users data based on user_id
Join_node = Join.apply(
    frame1=Workout_log_node,
    frame2=Users_node,
    keys1=["user_id"],
    keys2=["user_id"],
    transformation_ctx="Join_node",
)
print("Join Successful")

# Transform: Union the newly joined data with the exising warehouse data
Union_node = sparkUnion(
    glueContext,
    unionType="DISTINCT",
    mapping={"source1": Join_node, "source2": DW_node},
    transformation_ctx="Union_node",
)

print("Union Successful")

# Cleanup: Deleate all existing files in the warehouse path before writing the new data.
# List all objects in the specified S3 path
objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if 'Contents' in objects:
    for obj in objects['Contents']:
        s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
    print("Delete existing files successful")
else:
    print("No existing files to delete")

# Coalesce into a single partition before writing to S3
Coalesce_node = Union_node.coalesce(1)
print("Coalesce Successful")

# Load: Write the newly combined data back to warehouse as a Parquet file
AmazonS3_node = glueContext.write_dynamic_frame.from_options(
    frame=Coalesce_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://fitnesstracker-staging-264384440796-ap-southeast-1-an/warehouse/",
        "partitionKeys": [],
    },
    # Ensure correct format_options for Parquet (snappy compression)
    format_options={"compression": "snappy"},
    #format_options={"writeHeader": True, "separator": ",", "quoteChar": '"'},
    transformation_ctx="AmazonS3_node",
)
print("Save the data")
job.commit()