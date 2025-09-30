import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType
import openpyxl
import et_xmlfile

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1746209580034_df = pd.read_excel("s3://80shiphop/Glue_Jobs_details_by_1743446292449.xlsx")
AmazonS3_node1746209580034_schema = StructType([StructField(column_name, StringType(), True) for column_name in AmazonS3_node1746209580034_df.columns])
AmazonS3_node1746209580034_df = spark.createDataFrame(AmazonS3_node1746209580034_df, schema=AmazonS3_node1746209580034_schema)
AmazonS3_node1746209580034 = DynamicFrame.fromDF(AmazonS3_node1746209580034_df, glueContext, "AmazonS3_node1746209580034")

# Script generated for node Amazon S3
if (AmazonS3_node1746209580034.count() >= 10):
   AmazonS3_node1746209580034 = AmazonS3_node1746209580034.coalesce(10)
AmazonS3_node1746210176751 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1746209580034, connection_type="s3", format="xml", connection_options={"path": "s3://80shiphop", "partitionKeys": []}, transformation_ctx="AmazonS3_node1746210176751")

job.commit()