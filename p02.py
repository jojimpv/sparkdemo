import pyspark
from pyspark.sql import functions as func
from pyspark.sql.functions import col

spark = pyspark.sql.SparkSession.builder.getOrCreate()

try:
    data = spark.read \
        .format('csv') \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load('data.csv')

    data.select('State').distinct().count()
    data_clean = data.dropna()

    date_udf = func.udf(lambda x: x.split('/')[2])
    data_clean = data_clean.withColumn('year', date_udf(col('Quarter Ending')))
    data_clean = data_clean.drop('Quarter Ending')

finally:
    spark.stop()
