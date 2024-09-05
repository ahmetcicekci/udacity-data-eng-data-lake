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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1725567190945 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1725567190945")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1725568563852 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1725568563852")

# Script generated for node SQL Query
SqlQuery4151 = '''
select * from step_trainer_trusted 
join accelerometer_trusted
on step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timeStamp
'''
SQLQuery_node1725567206817 = sparkSqlQuery(glueContext, query = SqlQuery4151, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1725567190945, "step_trainer_trusted":StepTrainerTrusted_node1725568563852}, transformation_ctx = "SQLQuery_node1725567206817")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1725567414039 = glueContext.getSink(path="s3://spark-bucket-539727277989/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1725567414039")
MachineLearningCurated_node1725567414039.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1725567414039.setFormat("json")
MachineLearningCurated_node1725567414039.writeFrame(SQLQuery_node1725567206817)
job.commit()