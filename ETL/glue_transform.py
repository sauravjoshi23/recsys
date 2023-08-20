from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import concat_ws, when, lit, split, explode

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

ads_content_df = glueContext.create_dynamic_frame.from_catalog(
    database="recdb",
    table_name="ads_content_csv",
    transformation_ctx="ads_content_df"
).toDF()

ads_content_df = ads_content_df.withColumn(
    'content_combined', 
    concat_ws(' ', ads_content_df['title'], ads_content_df['description'], ads_content_df['tags'])
)

user_profiles_df = glueContext.create_dynamic_frame.from_catalog(
    database="recdb",
    table_name="user_profiles_csv",
    transformation_ctx="user_profiles_df"
).toDF()

user_ad_interaction_df = glueContext.create_dynamic_frame.from_catalog(
    database="recdb",
    table_name="user_ad_interactions_csv",
    transformation_ctx="user_ad_interaction_df"
).toDF()

user_ad_interaction_df = user_ad_interaction_df.withColumn(
    "action_value", 
    when(user_ad_interaction_df["action"] == "click", lit(1)).otherwise(lit(0.5))
)

user_ad_matrix = user_ad_interaction_df.groupBy("userId").pivot("adId").sum("action_value").fillna(0)

item_item_interaction_df = glueContext.create_dynamic_frame.from_catalog(
    database="recdb",
    table_name="item_item_interaction_csv",
    transformation_ctx="item_item_interaction_df"
).toDF()

transactions_df = glueContext.create_dynamic_frame.from_catalog(
    database="recdb",
    table_name="transactions_csv",
    transformation_ctx="transactions_df"
).toDF()
transactions_df = transactions_df.withColumn("adIds", split(transactions_df["adIds"], ","))
transactions_df = transactions_df.select("transactionId", "userId", explode(transactions_df["adIds"]).alias("adId"))
transactions_df = transactions_df.withColumn("present", lit(1))
transaction_matrix = transactions_df.groupBy("transactionId").pivot("adId").sum("present").fillna(0)


ads_content_glueDf = DynamicFrame.fromDF(ads_content_df, glueContext, "ads_content_glueDf")
user_profiles_glueDf = DynamicFrame.fromDF(user_profiles_df, glueContext, "user_profiles_glueDf")
user_ad_matrix_glueDf = DynamicFrame.fromDF(user_ad_matrix, glueContext, "user_ad_matrix_glueDf")
item_item_interaction_glueDf = DynamicFrame.fromDF(item_item_interaction_df, glueContext, "item_item_interaction_glueDf")
transaction_matrix_glueDf = DynamicFrame.fromDF(transaction_matrix, glueContext, "transaction_matrix_glueDf")

ads_content_write = glueContext.write_dynamic_frame.from_options(
    frame=ads_content_glueDf,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://recsys-aws/silver_data/ads_content_transformed/",
        "partitionKeys": []
    },
    transformation_ctx="ads_content_write"
)

user_profiles_write = glueContext.write_dynamic_frame.from_options(
    frame=user_profiles_glueDf,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://recsys-aws/silver_data/user_profiles_transformed/",
        "partitionKeys": []
    },
    transformation_ctx="user_profiles_write"
)

user_ad_matrix_write = glueContext.write_dynamic_frame.from_options(
    frame=user_ad_matrix_glueDf,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://recsys-aws/silver_data/user_ad_matrix/",
        "partitionKeys": []
    },
    transformation_ctx="user_ad_matrix_write"
)

item_item_write = glueContext.write_dynamic_frame.from_options(
    frame=item_item_interaction_glueDf,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://recsys-aws/silver_data/item_item_interaction_transformed/",
        "partitionKeys": []
    },
    transformation_ctx="item_item_write"
)

transaction_matrix_write = glueContext.write_dynamic_frame.from_options(
    frame=transaction_matrix_glueDf,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://recsys-aws/silver_data/transaction_matrix/",
        "partitionKeys": []
    },
    transformation_ctx="transaction_matrix_write"
)