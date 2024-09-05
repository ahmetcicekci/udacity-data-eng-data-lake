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

# Script generated for node LandedZone
LandedZone_node1724856313047 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://spark-bucket-539727277989/customer/landing/"], "recurse": True}, transformation_ctx="LandedZone_node1724856313047")

# Script generated for node SQL Query
SqlQuery2465 = '''
select * from myDataSource
where sharewithresearchasofdate is not null 

'''
SQLQuery_node1725372397558 = sparkSqlQuery(glueContext, query = SqlQuery2465, mapping = {"myDataSource":LandedZone_node1724856313047}, transformation_ctx = "SQLQuery_node1725372397558")

# Script generated for node TrustedZone
TrustedZone_node1724861631243 = glueContext.getSink(path="s3://spark-bucket-539727277989/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedZone_node1724861631243")
TrustedZone_node1724861631243.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
TrustedZone_node1724861631243.setFormat("json")
TrustedZone_node1724861631243.writeFrame(SQLQuery_node1725372397558)
job.commit()