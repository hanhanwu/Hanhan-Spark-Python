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

