import pyspark
from pyspark.sql import functions as func
from pyspark.sql.functions import col, broadcast
from pyspark.accumulators import AccumulatorParam

spark = pyspark.sql.SparkSession.builder.getOrCreate()

try:
    l1 = [('John', 1000), ('James', 2000), ('Nina', 1500)]
    l2 = [('John', 1), ('Nina', 3)]
    tb1 = spark.createDataFrame(l1, ['name', 'salary'])
    tb2 = spark.createDataFrame(l2, ['name', 'id'])
finally:
    spark.stop()
