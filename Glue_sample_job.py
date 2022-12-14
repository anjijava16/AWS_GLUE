import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "s3_data", table_name = "products_csv", transformation_ctx = "datasource0")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "s3_data", table_name = "sales_csv", transformation_ctx = "datasource1")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("productid", "long", "productid", "long"), ("productname", "string", "productname", "string"), ("price", "long", "price", "long")], transformation_ctx = "applymapping1")
applymapping2 = ApplyMapping.apply(frame = datasource1, mappings = [("productid", "long", "productid", "long"), ("invoicenumber", "long", "invoicenumber", "long")], transformation_ctx = "applymapping2")

resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
resolvechoice3 = ResolveChoice.apply(frame = applymapping2, choice = "make_cols", transformation_ctx = "resolvechoice3")

dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
dropnullfields4 = DropNullFields.apply(frame = resolvechoice3, transformation_ctx = "dropnullfields4")

transformation_join = Join.apply(frame1 = dropnullfields3, frame2 = dropnullfields4, keys1 = ["productid"], keys2 = ["productid"], transformation_ctx = "transformation_join")

result_df = SelectFields.apply(frame = transformation_join, paths = ["productid","productname","price","invoicenumber"], transformation_ctx = "result_df")

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = result_df, catalog_connection = "glue-redshift-new", connection_options = {"dbtable": "products_sales", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()
