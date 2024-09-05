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

# Script generated for node Customer Curated
CustomerCurated_node1725566186574 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1725566186574")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1725565998873 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1725565998873")

# Script generated for node SQL Query
SqlQuery4334 = '''
select * from step_trainer_landing 
where serialnumber in 
(select distinct serialnumber from customer_curated)
'''
SQLQuery_node1725566281443 = sparkSqlQuery(glueContext, query = SqlQuery4334, mapping = {"customer_curated":CustomerCurated_node1725566186574, "step_trainer_landing":StepTrainerLanding_node1725565998873}, transformation_ctx = "SQLQuery_node1725566281443")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1725566320481 = glueContext.getSink(path="s3://spark-bucket-539727277989/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1725566320481")
StepTrainerTrusted_node1725566320481.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1725566320481.setFormat("json")
StepTrainerTrusted_node1725566320481.writeFrame(SQLQuery_node1725566281443)
job.commit()