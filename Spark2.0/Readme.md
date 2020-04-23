I was using Spark 1.5. Now Spark 2.0 has make many changes and in fact it should be easier to use and faster for big data operations. Here, I'm creating this new folder, recording my Spark 2.0 practicing experience

## INSTALL & RUN SPARK 2.0

### Solutions for local machine, cluster, Hadoop VMs
* If you want to install/run it in different situations, such as in your local machine, Hadoop VMs, Cluster, etc. Check [this SFU CS Big Data Guidance][1]
* <b>NOW! You finally can simply install Spark through pip!</b>, type `pip install pyspark` in your terminal. I tried, now I can use spark in my IPython much easier
  * Make sure that `JAVA_HOME` is pointing to JDK8, Spark cannot compile with JDK 9+
    * Type `/usr/libexec/java_home -V` in the terminal, to find what JDK HOMES you have already installed
  * Also download Spark
  * Before running iPython, type:
    * `export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home`
    * `export SPARK_HOME=/Users/hanhan.wu/Downloads/spark-2.4.4-bin-hadoop2.7/`
    * `export PYSPARK_PYTHON=python3`
* <b>OR, An Old Method - To add Spark in PyCharm</b>
  1. I'm using [PyCharm][2], it is great
  2. [Download Spark][3], at the time I'm wrinting this guidance, I am using `spark-2.1.1-bin-hadoop2.7`
  3. Open your PyCharm, create a new project. Open PyCharm Preference, and find `Project Structure`. Click `Add Content Root`, and find the path to `[your spark download folder]/spark-2.1.1-bin-hadoop2.7/python/lib`, add `py4j-0.10.4-src.zip`, `pyspark.zip`. Then you should be able to use Spark in your PyCharm
* To run your .py file that's using Spark
  * Even if you have Spark in IDE, still need to run it through terminal. So, open your terminal
  * Type `export SPARK_HOME=[your spark download folder path]/spark-2.1.1-bin-hadoop2.7`
  * Type `${SPARK_HOME}/bin/spark-submit --master local [your python file].py [input file 1]`, input file is optional, all depends on how you are going to read the data input in your code. And you can set multiple input files
* For more detailed configuration, check [pyspark for beginners][21]

### [Install Spark on Windows][18]

## Practice
### [How to define `spark`][19]
* [Configuration Properties][20]

### Anomalies Detection (offline vs Streaming)
* I'm planning to test Spark2.0 streaming, to see whether it can make real time data analysis, and hope to find a way to check model quality
* Anomalies detection, OFFLINE (without streaming)
  * [Data Sample - parquet files][4]
  * [What does sample data look like][9], since you cannot read parquet directly
  * [My Code: Spark 2.0 Anomalies Detection code - With OO Design][5]
    * Compared with Spark1.5, one of the major changes is, `SqlContext` has been replaced with `SparkSession`, in the code we call it as `spark`. Meanwhile, `spark context` can be got from `spark.sparkContext`. If I didn't remember wrong, the reason they made this change is to make calling spark sql easier. You can simple use created `spark` to do many things that originally needed more libraries
  * [My Code: Spark 2.0 Anomalies Detection - IPython][11] - more clear to see input and output
    * [Same Data Input as above - parquet files][4]
    * Uses k-means clustering, then clusters with less member counts (higher score, higher than a threshold) will be considered as anomalies

* Anomalies detection, with streaming
  * When it comes to real time detection experiments, you can try these methods with Spark
    * [Spark Streaming][6] - Apply Spark streaming machine learning methods on streaming data
    * [Structured Streaming][7] - This one is still in Alpha Experimental stage, they are trying to allow you use streaming just like offline spark code
    * Offline trained model for online data - If your coming data do not have significant changes, then train your model with historical data offline, and apply this model on online data, but need to check your model quality periodically, to make sure it still works fine
    
  * Experiment 0 - Spark Streaming Basics
    * [Local Network Connection][13]
    
  * Experiment 1 - Spark Streaming
    * [How Spark Streaming K-Means Work][12]
      * For each batch of data, it applies 2 steps of k-means
        * Random initialize k centers
        * Generate clusters, compute the average for each cluster and update the new centers
      * [My Code: Spark streaming k-means][14]
        * [Sample training data][15]
        * [Sample testing data][16]
        * With `predictOnValues`, you need label and features, and you need `LabelPoint(label, feature_vector)` to put them together, vector has to be `Vectors.dense(python_list)`
        * With `predictOn`, you don't need the label, but just to have `Vectors.dense(python_list)` to put features together
        * dimension in `setRandomCeters()` means the number of features in a row
      * [My Code: Spark Streaming Anomalies Detection][17]
        * [Same Data Input as above Offline Anomalies Detection - parquet files][4]
        * [What does sample data look like][9], since you cannot read parquet directly
    * [Spark streaming k-means example][8]
    * [streaming k-means built-in methods][10]


### Spark Machine Learning Pipeline
* [My Code Example][26]
* Notes
  * It's definitely more organized to use machine learning pipeline, but I don't like to use it for spark streaming, it often creates issues in schema, since you have to convert streaming rdd to dataframe before using the pipeline.
  * In spark pipeline, you can use `transform()` function for all stages, but doesn't mean each stage supports `transform()` function. Although this makes pipeline better....
    * For example, `Word2Vec` doesn't support `transform()` but it can be used in a pipeline which will use transform...


### Spark Streaming
#### Terminology
* Caching
  * You can cach the data temporarily to avoid recompute. DStream allows you to keep the data in the memory.
* Checkpointing
  * If you don't have a large RAM, caching cannot work for large memory storage. "Checkpointing is another technique to keep the results of the transformed dataframes. It saves the state of the running application from time to time on any reliable storage like HDFS. However, it is slower and less flexible than caching."
* Shared Variable - it's the variable copied into each machine, used by that machine but also helps the communication across machines. For example, "each cluster’s executor will calculate the results of the data present on that particular cluster. But we need something that helps these clusters communicate so we can get the aggregated result. In Spark, we have shared variables that allow us to overcome this issue."
* Accumulator Variable
  * The executor on each cluster sends data back to the driver process to update the values of the accumulator variables. 
  * Accumulators are applicable only to the operations that are associative and commutative. For example, sum and maximum will work, whereas the mean will not.
* Broadcast Variable
  * Broadcast variables allow the programmer to keep a read-only variable cached on each machine. 
  * Usually, Spark automatically distributes broadcast variables using efficient broadcast algorithms but we can also define them if we have tasks that require the same data for multiple stages.
  
#### Available Spark MLLib Streaming Models
* Benefits with Built-in Streaming Methods
  * It seems that they don't need Netcat and can load text file as input themselves.
  * They have `trainOn()`, `predictOn()` methods instead of using `fit` & `transform`
* The limitation is, Spark has limited streaming machine learning models available.
* Classification
  * [Streaming Linear Regression][22]
* Clustering
  * [Streaming Kmeans][23]
  
#### Spark Streaming without Built-in Models
* [Twitter Sentiment Streaming][27]

#### How To Load Streaming Input
* With Netcat
  * `nc -lk [port number]` is how you turn on Netcat
  * [Manually Input From Netcat][24] - Once you turned on Netcat in your terminal, just type whatever text in that terminal, spark streaming will process that.
* Load File
  * [Read File as Streaming][24]
    * You don't really need to turn on Netcat.
    * Have to use rdd to iterate each line.
* With Cliet Streaming
  * [How to use socket to listen to client's streaming and process it][25]
* `ssc.awaitTermination` vs `ssc.stop`
  * Using `ssc.stop`, the sleeping time decides when to stop the streaming. Too early you may not process all the data; too late, you will get empty output after processing all the data.
  * Using `ssc.awaitTermination` will keep running until you terminate the program. Spark doesn't know when to stop the program even after all the data has been processed.
  
## Current Spark Limitations
* It's odd. Same SparkContext creation code, sometimes I can re-run in IPython multiple times, sometimes it will show me the error saying a duplicated Spark Context is running...
* After running streaming, original RDD or dataframe will become None. Also after spark context stopped after streaming, you need to re-create a spark context. So it's almost to run-run the code.
* Spark dataframe is not iterable, you need to iterate rdd, `df.rdd.collect()` is iterable
* Spark pipeline has to process dataframe, while streaming data is rdd
* In spark pipeline, you can use `transform()` function for all stages, but doesn't mean each stage supports `transform()` function. Although this makes pipeline better....
* There is spark `ml` and `mllib`, they could have functions with the same name but support different methods, which tend to cause confusion. When I was using spark pipeline, need to use functions in `ml`, not sure why they could not combine these 2 as 1
* I personally think, spark is not ready for advanced machine learning yet but more about a data enginerring tool at this moment. Because of its limited machine learninglibrary, and special data structure is not really comptable with other python machine learning libraries.

## Spark 2.* Basics
### Saprk SQL
* [This is a good starting point][28]
  * Use spark dataframe to run spark sql
  * Register temptable so that you can write spark SQL query
  * Briefly mentioned why and how spark sql is efficient
  
## Spark & Other Libraries
* [Histograms and Density Plots Visualization with Spark DataFrame][29]


[1]:https://courses.cs.sfu.ca/2016fa-cmpt-732-g5/pages/RunningSpark
[2]:https://www.jetbrains.com/pycharm/download/#section=mac
[3]:https://spark.apache.org/downloads.html
[4]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/logs-features-sample.zip
[5]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/anomalies_detection.py
[6]:https://spark.apache.org/docs/2.1.0/streaming-programming-guide.html#overview
[7]:https://spark.apache.org/docs/2.1.0/structured-streaming-programming-guide.html
[8]:http://spark.apache.org/docs/latest/mllib-clustering.html#streaming-k-means
[9]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/anomalies_detection_data_sample.txt
[10]:http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.clustering.StreamingKMeans
[11]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/Saprk_anomalies_detction.ipynb
[12]:https://databricks.com/blog/2015/01/28/introducing-streaming-k-means-in-spark-1-2.html
[13]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/spark_streaming_word_count.ipynb
[14]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/spark_kmeans_streaming.ipynb
[15]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/kmeans_train.csv
[16]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/kmeans_test.csv
[17]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/spark_streaming_anomalies_detection.ipynb
[18]:https://github.com/hanhanwu/Basic_But_Useful/blob/master/RA_command_lines.md#how-to-install-spark-on-windows
[19]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/how_to_define_spark.py
[20]:https://spark.apache.org/docs/latest/configuration.html#memory-management
[21]:https://www.analyticsvidhya.com/blog/2019/10/pyspark-for-beginners-first-steps-big-data-analysis/?utm_source=feedburner&utm_medium=email&utm_campaign=Feed%3A+AnalyticsVidhya+%28Analytics+Vidhya%29
[22]:https://spark.apache.org/docs/latest/mllib-linear-methods.html#streaming-linear-regression
[23]:https://spark.apache.org/docs/latest/mllib-clustering.html#streaming-k-means
[24]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/simulate_streaming_input.ipynb
[25]:https://towardsdatascience.com/hands-on-big-data-streaming-apache-spark-at-scale-fd89c15fa6b0
[26]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/spark_MLPipeline.ipynb
[27]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/streaming_twitter_sentiment.ipynb
[28]:https://www.analyticsvidhya.com/blog/2020/02/hands-on-tutorial-spark-sql-analyze-data/?utm_source=feedburner&utm_medium=email&utm_campaign=Feed%3A+AnalyticsVidhya+%28Analytics+Vidhya%29
[29]:https://github.com/Bergvca/pyspark_dist_explore
