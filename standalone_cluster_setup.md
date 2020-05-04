# Add /conf/spark-env.sh:

    export SPARK_WORKER_MEMORY=1g
    export SPARK_EXECUTOR_MEMORY=512m
    export SPARK_WORKER_INSTANCES=2
    export SPARK_WORKER_CORES=2
    export SPARK_WORKER_DIR=/home/joji/works/sparkdata

## Enable conf/slaves with default:

`localhost`

## Start master:

`sbin/start-master.sh`

## Start slave:

`sbin/start-slaves.sh`

## PS Check:

    joji@joji-HP-Pavilion-Laptop-14-bf1xx:~/spark$ ps -ef | grep spark.deploy | grep -v grep
    joji        3555       1  0 16:52 pts/1    00:00:06 /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -cp /home/joji/spark/conf/:/home/joji/spark/jars/* -Xmx1g org.apache.spark.deploy.master.Master --host joji-HP-Pavilion-Laptop-14-bf1xx --port 7077 --webui-port 8080
    joji        5454       1  1 17:38 ?        00:00:04 /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -cp /home/joji/spark/conf/:/home/joji/spark/jars/* -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://joji-HP-Pavilion-Laptop-14-bf1xx:7077
    joji        5540       1  1 17:39 ?        00:00:04 /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -cp /home/joji/spark/conf/:/home/joji/spark/jars/* -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8082 spark://joji-HP-Pavilion-Laptop-14-bf1xx:7077
    joji@joji-HP-Pavilion-Laptop-14-bf1xx:~/spark$ 

## Spark Shell:

`bin/spark-shell –master spark://joji-HP-Pavilion-Laptop-14-bf1xx:7077  # Scala shell`

    export PYSPARK_DRIVER_PYTHON=ipython
    export PYSPARK_PYTHON=python3.7
    bin/pyspark --master spark://joji-HP-Pavilion-Laptop-14-bf1xx:7077
    sc.getConf().getAll()
    
    data = range(10)
    dist_data = sc.parallelize(data)
    print(dist_data.reduce(lambda a, b: a+b))
    

## Stop processes:

    sbin/stop-slaves.sh
    sbin/stop-master.sh

        