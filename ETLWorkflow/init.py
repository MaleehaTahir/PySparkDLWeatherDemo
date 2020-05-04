from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from py4j.java_gateway import java_import


# ok lets create a spark session
# you can set additional configs, however this would be dependent on the local resources you have available
# Please note that, in local mode, the spark master and executors (slaves) run locally within a single JVM,
# thus while the local build is appropriate for a small workload, in real world scenarios, you would
# probably have to define runtime properties for a more scalable approach.
def SparkSessionBuild(app_name="HelloWorld", master='local'):
    try:
        session = SparkSession.builder.master(master).appName(app_name)\
            .config('spark.executor.memory', '8gb')\
            .config('spark.cores.max", "6')\
            .getOrCreate()
    except Py4JJavaError as e:
        raise e

    return session


sc = SparkSessionBuild()

# setting hadoop properties with key value pairs
# The aim of setting these properties is to build a bridge between pyspark and hdfs via java
# scala doesn't have this issue!!!
java_import(sc._jvm, 'org.apache.hadoop.fs.Path')
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata.level", "false")
sc._jsc.hadoopConfiguration().set("spark.sql.sources.partitionColumnTypeInference.enabled", "true")
sc._jsc.hadoopConfiguration().set("spark.sql.execution.arrow.enabled","true")