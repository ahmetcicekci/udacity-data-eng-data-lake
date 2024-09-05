import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1725429975875 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1725429975875")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1725429964493 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1725429964493")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1725430035913 = Join.apply(frame1=CustomerTrusted_node1725429975875, frame2=AccelerometerLanding_node1725429964493, keys1=["email"], keys2=["user"], transformation_ctx="CustomerPrivacyFilter_node1725430035913")

# Script generated for node Drop Fields
DropFields_node1725430397228 = DropFields.apply(frame=CustomerPrivacyFilter_node1725430035913, paths=["email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "customername"], transformation_ctx="DropFields_node1725430397228")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1725430079349 = glueContext.getSink(path="s3://spark-bucket-539727277989/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1725430079349")
AccelerometerTrusted_node1725430079349.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1725430079349.setFormat("json")
AccelerometerTrusted_node1725430079349.writeFrame(DropFields_node1725430397228)
job.commit()