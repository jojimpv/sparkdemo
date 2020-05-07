import sys

import pyspark
from pyspark.streaming import StreamingContext


student_data_file = 'student.csv'
spark = pyspark.sql.SparkSession.builder.appName('SparkStreamDemo').getOrCreate()

try:
    sc = spark.sparkContext
    ssc = StreamingContext(sc, batchDuration=2)
    ssc.checkpoint('file:///tmp/ssc2')
    lines_ds = ssc.socketTextStream('localhost', 9000)
    lines_sum = lines_ds.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
    lines_sum.pprint()
    ssc.start()
    ssc.awaitTermination()

finally:
    spark.stop()
