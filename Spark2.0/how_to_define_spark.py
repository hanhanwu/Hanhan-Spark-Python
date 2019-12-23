# In Spark 2.*, I found it requires differnt methods to define Spark

# Method 1 - Spark Session from Spark Context (most common way)
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.fpm import FPGrowth

sc = SparkContext('local[2]', appName='local')  # local n specifies n threads
spark = SparkSession(sc)  # defined saprk

df = spark.createDataFrame([
    (0, [1, 2, 5]),
    (1, [1, 2, 3, 5]),
    (2, [1, 2])
], ["id", "items"])


# Method 2 - Spark Context from Saprk Session
## Used in my code here: https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/anomalies_detection.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import Row
import operator
from pyspark.mllib.clustering import KMeans

spark = SparkSession.builder \
        .master("local") \
        .appName("Anomalies Detection") \
        .config("spark.some.config.option", "some-value") \  
        .getOrCreate()

sparkCt = spark.sparkContext

# config properties: https://spark.apache.org/docs/latest/configuration.html#available-properties
# For example: I had a giant file, and it reported errors related to GC (garbage collection), 
## but in fact I just need to set driver size larger and cache() the large dataframe
spark = SparkSession.builder.master("local")\
       .appName("Try Association Rules")\
       .config("spark.driver.memory", "4g")\
       .getOrCreate()

df = spark.createDataFrame(item_lst, ["id", "items"])
df.cache()  # cache the giant dataframe will help
