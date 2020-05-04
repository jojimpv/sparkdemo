import time
from pyspark.sql import SparkSession

# http://localhost:4040
data_file = 'data.csv'
search_word = 'Health'


def word_count(spark):
    print('Sleeping for 10s before read')
    time.sleep(10)
    df_file_data = spark.read.text(data_file).cache()
    print('Sleeping for 10s after read')
    time.sleep(10)
    count_word = df_file_data.filter(df_file_data.value.contains(search_word)).count()
    print('Sleeping for 10s before results')
    time.sleep(10)
    print(f'Line count with word "{search_word}" = {count_word}')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('sparkdemo').getOrCreate()
    try:
        word_count(spark)
    finally:
        spark.stop()
