# Spark

## Overview

Main abstractions:
- RDD (Resilient Distributed Dataset):
    - collections of elements in nodes in cluster
    - recover from node failure
- Shared variables for parallel processing:
    - broadcast variables:
        - used to cache a variable indistra all nodes
    - accumulators:
        - only added such as counter

## Linking with Spark:

- To run `bin/spark-submit`
- To launch an interactive Python shell `bin/pyspark`
- Need same minor version of Python in both driver and workers:
    - Default to use python in PATH
    - `PYSPARK_PYTHON=python3.4 bin/pyspark`
- Start with spark context:

        from pyspark import SparkContext, SparkConf
        conf = SparkConf().setAppName(appName).setMaster(master)
        sc = SparkContext(conf=conf)

    - master = 'local' for testing

## Spark Shell

- Spark context created as `sc` and session as `spark`
- pyspark Arguments:
    - `--master local[4]` [4] to use 4 cores
    - `--py-files code.py` this can be .egg, .zip, .py 
    - `--packages` comma-separated list of Maven coordinates
    - `--repositories` eg. Sonatype
- Help:
    - `pyspark --help`
- Ipython:
    - `PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark`
- Jupyter notebook:
    - `PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook ./bin/pyspark`
    - Add `%pylab inline` as first line

## RDD

- Fault-tolerant collection of elements that can be operated on in parallel.
- Can be created using:
    - `parallelizing` an existing collection in your driver program
    
            data = [1, 2, 3, 4, 5]
            distData = sc.parallelize(data)
    
    - referencing a dataset in an external storage system Eg. HDFS, HBase
- Read from file using `sc.textFile`:
    - Files referred need to be locally available in all nodes
    - Supports .zip, dir and wild char
    - Number of partitions can be requested more
- Other supported formats:
    - `SparkContext.wholeTextFiles` for `(filename, content)` data
    - `RDD.saveAsPickleFile and SparkContext.pickleFile`to save RDD to serialized python objects
    - SequenceFile and Hadoop Input/Output Formats Eg. read from Elastic search or Casandra
- RDD Operations:
    - Transformations:
        - `map` process each element in a dataset and return new RDD
    - Actions
        - `reduce` aggregate and return final result to driver program. 
           Exception `reduceByKey` which return distributed data set
- Spark transformations are lazy so use `cache` or `persist` if frequent access needed

### Passing Functions to Spark

1. Lambda functions
1. Local defs
1. Top level functions in module
 
- Spark compute and send closure(methods and variables) which need to be visible for executers
- Use `Accumulator` for sharing common data
- Printing elements in RDD need to be done after `collect`:
    - `rdd.collect().foreach(println)`
    - `rdd.take(100).foreach(println)` 
- some operators are available for key value pairs like `reduceByKey` `sortByKey`
- List of transformations [API](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD)
- List of actions [API](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD)

### Shuffle operations

- Eg. `reduceByKey` need to bring elements from multiple partitions to one
- Eg. `repartitionAndSortWithinPartitions`    

### RDD Persistence

- Using `persist()` or `cache()`
- Config set by sending `StorageLevel` obj. Default `StorageLevel.MEMORY_ONLY`
- Choose `MEMORY_ONLY` or `MEMORY_ONLY_SER` (Java/Scala)
- Delete using `RDD.unpersist()`

### Data sharing 

- Broadcast variables; to avoid shipping `v` across multiple tasks:
        
        v = [1, 2, 3]
        broadcastVar = sc.broadcast(v)
        broadcastVar.value
       
- Accumulators:
        
        v = 0
        accum = sc.accumulator(v)
        accum.value
        
    - Note garentied 

## Spark Env

SPARK_CONF_DIR          - Conf dir other than spark/conf
PYSPARK_PYTHON          - Python binary executable to use for PySpark in both driver and workers
PYSPARK_DRIVER_PYTHON   - Python binary executable to use for PySpark in driver only. Property spark.pyspark.driver.python take precedence if it is set
SPARK_LOCAL_IP          - IP address of the machine to bind to.

## Spark conf

        ./bin/spark-submit \ 
          --name "My app" \ 
          --master local[4] \  
          --conf spark.eventLog.enabled=false \ 
          --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \ 
          --conf spark.hadoop.abc.def=xyz \ 
          myApp.jar

        >>> spark = SparkSession.builder \\
        ...     .master("local") \\
        ...     .appName("Word Count") \\
        ...     .config("spark.some.config.option", "some-value") \\
        ...     .getOrCreate()

        from pyspark import SparkConf
        sconf = SparkConf()
        sconf.set('spark.some', 'som val')        

## Spark execution

- For Python applications, simply pass a .py file in the place of <application-jar> instead of a JAR, 
  and add Python .zip, .egg or .py files to the search path with --py-files.

## Spark Cluster 

- Cluster managers:
    - Standalone – a simple cluster manager included with Spark that makes it easy to set up a cluster.
    - Apache Mesos – a general cluster manager that can also run Hadoop MapReduce and service applications.
    - Hadoop YARN – the resource manager in Hadoop 2.
    - Kubernetes – an open-source system for automating deployment, scaling, and management of containerized applications.
    


    
    