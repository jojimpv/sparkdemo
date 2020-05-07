from datetime import datetime

import pyspark
from pyspark import Row
from pyspark.sql import functions as func
from pyspark.sql.functions import col, broadcast
from pyspark.accumulators import AccumulatorParam

spark = pyspark.sql.SparkSession.builder.getOrCreate()

try:
    rows = [
        Row(id=1,
            name='Jon',
            active=True,
            clubs=['chess', 'hockey'],
            marks={'maths': 95, 'hindi': 88},
            enrolled=datetime(2020, 4, 20, 10, 0, 0)
            ),
        Row(id=2,
            name='Jan',
            active=False,
            clubs=['dance', 'music'],
            marks={'maths': 90, 'hindi': 90},
            enrolled=datetime(2019, 4, 20, 10, 0, 0)
            )
    ]
    sc = spark.SparkContext
    rdd = sc.parallelize(rows)
    df = rdd.toDF()
    df.createOrReplaceTempView('records')
    df1 = spark.sql('select * from records')
    spark.sql('select name, clubs[1], marks["maths"] from records where active=True').show()
    spark.sql('select name, clubs[1], marks["maths"] from records where active and marks["maths"]> 90').show()
    df.createOrReplaceGlobalTempView('global_records')
    spark.sql('select name, clubs[1], marks["maths"] from global_temp.global_records where active=True').show()

finally:
    spark.stop()
