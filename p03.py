import pyspark
from pyspark.sql import functions as func
from pyspark.sql.functions import col, broadcast
from pyspark.accumulators import AccumulatorParam

spark = pyspark.sql.SparkSession.builder.getOrCreate()


class MyAccumulator(AccumulatorParam):
    """Class for custom accumulator"""
    def zero(self, value):
        return [0.0] * len(value)

    def addInPlace(self, value1, value2):
        for i in range(len(value1)):
            value1[i] += value2[i]

        return value1


sc = spark.SparkContext

vector_accm = sc.accumulator([10.0, 20.0, 30.0], MyAccumulator())
vector_accm.value

vector_accm += [1, 2, 3]

def fn_join(players: pyspark.sql.DataFrame, strikers: pyspark.sql.DataFrame):
    player_details = players.join(strikers, players.pid == strikers.pid)
    player_details = players.join(strikers, ['pid'])
    striker_details = players.select('pid', 'pname').join(strikers, ['pid'])
    striker_details = players.select('pid', 'pname').join(broadcast(strikers), ['pid'])
    striker_details.select('pname', 'pid')\
                   .coalesce(1)\
                   .write\
                   .option('header', 'true')\
                   .csv('striker_data.csv')  # this is a dir name




try:
    fn_join()

finally:
    spark.stop()
