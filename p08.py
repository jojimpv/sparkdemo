import pyspark
from pprint import pprint

student_data_file = 'student.csv'
# spark = pyspark.sql.SparkSession.builder.appName('SparkStreamDemo').master().getOrCreate()
spark = pyspark.sql.SparkSession.builder.master('spark://ALIPL1073.local:7077').getOrCreate()

try:
    pprint(spark.sparkContext.getConf().getAll())
    student_df = spark.read.csv('student.csv', inferSchema=True, header=True)
    student_df4 = student_df.repartition(4)
    student_df4.write.save('student_data4', mode='overwrite')

finally:
    spark.stop()
