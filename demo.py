import pyspark
from pyspark.sql import SparkSession

data_file = 'data.csv'
search_word = 'Health'


def word_count(spark):
    df_file_data = spark.read.text(data_file).cache()
    count_word = df_file_data.filter(df_file_data.value.contains(search_word)).count()
    print(f'Line count with word "{search_word}" = {count_word}')


def rdd_sum_len(sc):
    rdd = sc.textFile('data.csv')
    total_len = rdd.map(lambda x: len(x)).reduce(lambda a, b: a + b)
    print(f'Sum of line len = {total_len}')


def rdd_to_seq_file(sc: pyspark.SparkContext):
    seq_file = 'rdd_seq.bin'
    rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, 'z'*x))
    rdd.saveAsSequenceFile(seq_file)
    print('Sequence file read: %s' % sc.sequenceFile(seq_file).collect())


def rdd_list_sum(sc: pyspark.SparkContext):
    l = range(1, 4)
    rdd = sc.parallelize(l)
    lsum = rdd.reduce(lambda a, b: a+b)
    print(f'Sum of l({list(l)}) = {lsum}')


counter = 0


def counter_issue(sc):
    def add_counter(element):
        global counter
        counter += 1

    rdd = sc.parallelize(range(1, 4))
    rdd.foreach(add_counter)
    print(f'Final counter = {counter}')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('sparkdemo').getOrCreate()
    try:
        sc = spark.sparkContext
        # word_count(spark)
        # rdd_sum_len(sc)
        # rdd_to_seq_file(sc)
        rdd_list_sum(sc)
        # counter_issue(sc)
    finally:
        spark.stop()
