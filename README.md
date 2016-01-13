# Spark-Python
Spark Python code I have wrote

wordcount-improved.py and reddit-averages.py

These 2 files are doing the same works like WordCountImproved.java, RedditAverages.java in my Hadoop-MapReduce folder, but instead of using java MapReduce, it's using Spark core python to do MapReduce works


correlate-logs.py
This file does the same work as CorrelateLogs.java I wrote in HBase-MapReduce-in-Java folder, but instead of using java MapReduce, I am using Spark core python.


correlate-logs-better.py
This file does the same work as correlate-logs.py but it is using a better formula which is mroe numerical stable https://courses.cs.sfu.ca/2015fa-cmpt-732-g1/pages/Assignment3B


itemsets.py

This file is using Spark MlLib FP-Growth (a Frequent Pattern Mining algorithm) to find the top 10,000 item sets.
The transaction data input is from here: http://fimi.ua.ac.be/data/T10I4D100K.dat


relative-score.py

This file calculates the best comment on Redit, it is looking for the largest values of (comment score)/(subreddit average score)


relative-score-bcast.py

This file does the similar work as relative-scire.py, but deals with huge data input. When the data is huge and join is expensive, it is using Spark core python broadcast join. Once you broadcast a value, it is sent to each executor. You can access it by passing around the broadcast variable and any function can then get the .value property out of it.

To calculate the relative score, we map over the comments: write a function that takes the broadcast variable and comment as arguments, and returns the relative average as before. This technique is a broadcast join.


 reddit_average_sql.py
 
 This file does the same work like RedditAverage.java in my Hadoop-MapReduce folder but instead of using java, it is using Spark SQL.
 
 
 load_logs_sql.py
 
 This file does the same work as LoadLogs.java in my Hadoop-MapReduce folder but instead of using java, it is using Spark SQL.
 
 temp_range.py, temp_range_sql.py
 
 These 2 files look at look at historical weather data from the Global Historical Climatology Network. Their archives contain (among other things) lists of weather observations from various weather stations formatted as CSV files like this:

USC00242347,20120105,TMAX,133,,I,K,0800

USC00242347,20120105,TMIN,-56,,,K,0800

USC00242347,20120105,TOBS,-56,,,K,0800

US1SCPC0003,20120105,PRCP,0,,,N,

US1SCPC0003,20120105,SNOW,0,,,N,

NOE00133566,20120105,TMAX,28,,,E,

NOE00133566,20120105,TMIN,-26,,,E,

NOE00133566,20120105,PRCP,57,,,E,

NOE00133566,20120105,SNWD,0,,,E,

The fields are the weather station; the date (Jan 5 2012 in these lines); the observation (min/max temperature, amount of precipitation, etc); the value (and integer); and four more columns we won't use. The readme file in the archive explains the fields in detail. We will worry about the min and max temperature (TMIN, TMAX) which are given as °C × 10.

The problem they are trying to solve is: what weather station had the largest temperature difference on each day? That is, where was the largest difference between TMAX and TMIN?

temp_range.py is using Spark core python while temp_range_sql.py is using Spark sql.

 
 shortest_path.py
 
 This file is using Spark Sql to calculate Dijkstra's algorithm - Shortest Path.
 
The data input will represent the graph by listing the outgoing edges of each node (and all of our nodes will be labelled with integers):

1: 3 5

2: 4

3: 2 5

4: 3

5:

6: 1 5
 
 
read_stream.py

This file work with Spark Streaming to process a constant stream of data in real(-ish) time.

In order to produce some streaming data, I have set up a simple TCP server on cmpt732.csil.sfu.ca that returns (x,y) points as plain text lines like this:

103.492239 100.839233

883.152476 878.051790

-331.287403 -333.856794

The the server is simple: it generates random points near a line. This will simulate a stream of sensor data.

It does a simple linear regression on the data and store the slope (which the Wikipedia page calls α̂ ) and intercept (β̂ ) of the values. https://en.wikipedia.org/wiki/Simple_linear_regression


movie_recomendations.py

Recommended movies using Spark MLlib Collaborative Filtering. To register the user-entered titles with Tweetings, use the Tweetings title with the smallest Levenshtein distance from the user's entry. There is a Levenshtein implementation in pyspark.sql

Sample user's ratings:

10 The Lord of the Rings: The Return of the King

8 The Lord of the Rings: The Fellowship of the Ring (2001)

5 orrest Gump (1994)

9 Mad Max: Fury Road (2014)

3 Mad Max (1979)

The dataset is from here: https://github.com/sidooms/MovieTweetings


euler.py

This file does the same work as what I did in Hadoop-MapReduce folder, EvaluateEuler.java, but instead of using java MapReduce, it is using python sql.

*********************************************************************************
Spark MLlib and Spark ML

1. matrix_multiply.py
 * Check data input in data_matrix.txt. 
 Note: The original input data is huge and could not be uploadded to GitHub. The code is also good for n rows data input when n is huge.
 * The code in matrix_multiply.py is using Spark python to calculate transpose(A)*A, A is a matrix (input).
 * Instead of using the traditional way to calculate matric multiply, here I am using the theory of outer product, https://en.wikipedia.org/wiki/Matrix_multiplication#Outer_product
 * It means, no matter how large n (row) will be, if the number of columns of a matrix can be put on a single machine, we can just parallely use an individual column of tranpose(A) to multiply the relative row of A, and finally add all the matrics up.

2. matrix_multiply_sparse.py
 * The code here is using the same idea for matrix multiply, but is desiged for handling sparse matrix, by taking advantage of python csr_matrix.

3. spark_ml_pipline.py
 * The code in this python file is to get to know Spark machine learning pipeline.
 * Using cross validation to tune parameters, getting the best prediction model
