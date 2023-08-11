from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import udf

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

glueDf = glueContext.create_dynamic_frame.from_catalog(
    database="recdb",
    table_name="raw_data",
    transformation_ctx="glueDF"
)

df = glueDf.toDF()
df_transformed = df.drop("Timestamp")

df_glueDf = DynamicFrame.fromDF(df_transformed, glueContext, "df_glueDf")

df_write = glueContext.write_dynamic_frame.from_options(
    frame=df_glueDf,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://recsys-aws/silver_data/",
        "partitionKeys": []
    },
    transformation_ctx="df_write"
)