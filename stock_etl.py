import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import LongType 
from pyspark.sql.functions import col, lower, regexp_replace, trim 
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from Glue Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(database="db_datapipeline", table_name="original_data")

# Convert to DataFrame
df = datasource.toDF()

# Print schema to verify column names (optional)
df.printSchema()

# Data Processing
# Filter out rows where 'date' is null
df = df.filter(col('date').isNotNull())

# Capturing last 6 years data to find the recent trends in the stock market
df = df.filter((col('date') >= '2016-01-01') & (col('date') <= '2021-12-31'))

# Filter out rows where any column is null
for x in df.columns:
    df = df.filter(col(x).isNotNull())

# Remove duplicate rows
df = df.dropDuplicates()

# Save cleaned data to S3
df.write.format('csv').option('header', 'true').mode('overwrite').save('s3://bigdataprojectetlpipeline/processed_data/')

# Print the first few rows to verify
df.show(5)

# Commit the job
job.commit()