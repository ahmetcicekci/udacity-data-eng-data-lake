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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1725545441613 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1725545441613")

# Script generated for node Customer Trusted
CustomerTrusted_node1725545405602 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1725545405602")

# Script generated for node SQL Query
SqlQuery4567 = '''
SELECT *
FROM customer_trusted
WHERE email IN (
		SELECT DISTINCT user
		FROM accelerometer_landing
	)
'''
SQLQuery_node1725545493666 = sparkSqlQuery(glueContext, query = SqlQuery4567, mapping = {"accelerometer_landing":AccelerometerLanding_node1725545441613, "customer_trusted":CustomerTrusted_node1725545405602}, transformation_ctx = "SQLQuery_node1725545493666")

# Script generated for node Customer Curated
CustomerCurated_node1725545741455 = glueContext.getSink(path="s3://spark-bucket-539727277989/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1725545741455")
CustomerCurated_node1725545741455.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1725545741455.setFormat("json")
CustomerCurated_node1725545741455.writeFrame(SQLQuery_node1725545493666)
job.commit()