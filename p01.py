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

    data.printSchema()
    data.count()
    data.limit(5).show()
    data.dropna()  # drop missing data
    data.drop('UnitNo')
    data.printSchema()
    distinct_quarters = data.select('Quarter Ending').distinct()
    distinct_quarters.show()
    data.filter(col('Quarter Ending') == '06/30/2017')
    data.filter(col('Quarter Ending').isin(['06/30/2017', '09/30/2002']))
    data.sample(fraction=0.1).show()
    data.groupBy('Department').count().show()
    data.groupBy('Department', 'Vendor').agg({'Amount': 'sum'}).show()
    data.groupBy('Department', 'Vendor').agg({'Amount': 'sum'}).withColumnRenamed('sum(Amount)', 'sum_amount')
    data_sum = data.groupBy('Department', 'Vendor').agg({'Amount': 'sum'}).withColumnRenamed('sum(Amount)', 'sum_amount')
    total_sum = data_sum.agg({'sum_amount': 'sum'})
    grand_total = total_sum.collect()[0][0]
    data_sum.withColumn('prct', func.round(col('sum_amount') * 100 / grand_total, 2)).show()
    data_sum.withColumn('prct', func.round(col('sum_amount') * 100 / grand_total, 2)).orderBy(col('prct').desc()).show()
    data_sum_dept_desc = data_sum.withColumn('prct', func.round(col('sum_amount') * 100 / grand_total, 2)).orderBy(col('prct').desc())
    data.agg({'Quarter Ending': 'min'})
    data.toLocalIterator()

finally:
    spark.stop()
