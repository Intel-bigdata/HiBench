# HiBench Suite [![Build Status](https://travis-ci.org/intel-hadoop/HiBench.svg?branch=master)](https://travis-ci.org/intel-hadoop/HiBench)
## The bigdata micro benchmark suite ##


* Current version: 7.1
* Homepage: https://github.com/intel-hadoop/HiBench
* Contents:
  1. Overview
  2. Getting Started
  3. Workloads
  4. Supported Releases

---
### OVERVIEW ###

HiBench is a big data benchmark suite that helps evaluate different big data frameworks in terms of speed, throughput and system resource utilizations. It contains a set of Hadoop, Spark and streaming workloads, including Sort, WordCount, TeraSort, Repartition, Sleep, SQL, PageRank, Nutch indexing, Bayes, Kmeans, NWeight and enhanced DFSIO, etc. It also contains several streaming workloads for Spark Streaming, Flink, Storm and Gearpump.

### Getting Started ###
 * [Build HiBench](docs/build-hibench.md)
 * [Run HadoopBench](docs/run-hadoopbench.md)
 * [Run SparkBench](docs/run-sparkbench.md)
 * [Run StreamingBench](docs/run-streamingbench.md) (Spark streaming, Flink, Storm, Gearpump)

### Workloads ###

There are totally 27 workloads in HiBench. The workloads are divided into 6 categories which are micro, ml(machine learning), sql, graph, websearch and streaming.

  **Micro Benchmarks:**

1. Sort (sort)

    This workload sorts its *text* input data, which is generated using RandomTextWriter.

2. WordCount (wordcount)

    This workload counts the occurrence of each word in the input data, which are generated using RandomTextWriter. It is representative of another typical class of real world MapReduce jobs - extracting a small amount of interesting data from large data set.

3. TeraSort (terasort)

    TeraSort is a standard benchmark created by Jim Gray. Its input data is generated by Hadoop TeraGen example program.
    
4. Repartition (micro/repartition)
    
    This workload benchmarks shuffle performance. Input data is generated by Hadoop TeraGen. The workload randomly selects the post-shuffle partition for each record, performs shuffle write and read, evenly repartitioning the records. There are 2 parameters providing options to eliminate data source & sink I/Os: hibench.repartition.cacheinmemory(default: false) and hibench.repartition.disableOutput(default: false), controlling whether or not to 1) cache the input in memory at first 2) write the result to storage

5. Sleep (sleep)

    This workload sleep an amount of seconds in each task to test framework scheduler.

6. enhanced DFSIO (dfsioe)

    Enhanced DFSIO tests the HDFS throughput of the Hadoop cluster by generating a large number of tasks performing writes and reads simultaneously. It measures the average I/O rate of each map task, the average throughput of each map task, and the aggregated throughput of HDFS cluster. Note: this benchmark doesn't have Spark corresponding implementation.


**Machine Learning:**

1. Bayesian Classification (Bayes)

    Naive Bayes is a simple multiclass classification algorithm with the assumption of independence between every pair of features. This workload is implemented in spark.mllib and uses the automatically generated documents whose words follow the zipfian distribution. The dict used for text generation is also from the default linux file /usr/share/dict/linux.words.ords.

2. K-means clustering (Kmeans)

    This workload tests the K-means (a well-known clustering algorithm for knowledge discovery and data mining) clustering in spark.mllib. The input data set is generated by GenKMeansDataset based on Uniform Distribution and Guassian Distribution. There is also an optimized K-means implementation based on DAL (Intel Data Analytics Library), which is available in the dal module of sparkbench.

3. Logistic Regression (LR)

    Logistic Regression (LR) is a popular method to predict a categorical response. This workload is implemented in spark.mllib with LBFGS optimizer and the input data set is generated by LogisticRegressionDataGenerator based on random balance decision tree. It contains three different kinds of data types, including categorical data, continuous data, and binary data.

4. Alternating Least Squares (ALS)

    The alternating least squares (ALS) algorithm is a well-known algorithm for collaborative filtering. This workload is implemented in spark.mllib and the input data set is generated by RatingDataGenerator for a product recommendation system.

5. Gradient Boosted Trees (GBT)

    Gradient-boosted trees (GBT) is a popular regression method using ensembles of decision trees. This workload is implemented in spark.mllib and the input data set is generated by GradientBoostedTreeDataGenerator.

6. Linear Regression (Linear)

    Linear Regression (Linear) is a workload that implemented in spark.mllib with SGD optimizer. The input data set is generated by LinearRegressionDataGenerator.

7. Latent Dirichlet Allocation (LDA)

    Latent Dirichlet allocation (LDA) is a topic model which infers topics from a collection of text documents. This workload is implemented in spark.mllib and the input data set is generated by LDADataGenerator.

8. Principal Components Analysis (PCA)

    Principal component analysis (PCA) is a statistical method to find a rotation such that the first coordinate has the largest variance possible, and each succeeding coordinate in turn has the largest variance possible. PCA is used widely in dimensionality reduction. This workload is implemented in spark.mllib. The input data set is generated by PCADataGenerator.

9. Random Forest (RF)

    Random forests (RF) are ensembles of decision trees. Random forests are one of the most successful machine learning models for classification and regression. They combine many decision trees in order to reduce the risk of overfitting. This workload is implemented in spark.mllib and the input data set is generated by RandomForestDataGenerator.

10. Support Vector Machine (SVM)

    Support Vector Machine (SVM) is a standard method for large-scale classification tasks. This workload is implemented in spark.mllib and the input data set is generated by SVMDataGenerator.

11. Singular Value Decomposition (SVD)

    Singular value decomposition (SVD) factorizes a matrix into three matrices. This workload is implemented in spark.mllib and its input data set is generated by SVDDataGenerator.


**SQL:**

1. Scan (scan) 2. Join (join), 3. Aggregate (aggregation)

    These workloads are developed based on SIGMOD 09 paper "A Comparison of Approaches to Large-Scale Data Analysis" and HIVE-396. It contains Hive queries (Aggregation and Join) performing the typical OLAP queries described in the paper. Its input is also automatically generated Web data with hyperlinks following the Zipfian distribution.

**Websearch Benchmarks:**

1. PageRank (pagerank)

    This workload benchmarks PageRank algorithm implemented in Spark-MLLib/Hadoop (a search engine ranking benchmark included in pegasus 2.0) examples. The data source is generated from Web data whose hyperlinks follow the Zipfian distribution.

2. Nutch indexing (nutchindexing)

    Large-scale search indexing is one of the most significant uses of MapReduce. This workload tests the indexing sub-system in Nutch, a popular open source (Apache project) search engine. The workload uses the automatically generated Web data whose hyperlinks and words both follow the Zipfian distribution with corresponding parameters. The dict used to generate the Web page texts is the default linux dict file.

**Graph Benchmark:**

1. NWeight (nweight) 

    NWeight is an iterative graph-parallel algorithm implemented by Spark GraphX and pregel. The algorithm computes associations between two vertices that are n-hop away. 


**Streaming Benchmarks:**

1. Identity (identity)

    This workload reads input data from Kafka and then writes result to Kafka immediately, there is no complex business logic involved.

2. Repartition (streaming/repartition)

    This workload reads input data from Kafka and changes the level of parallelism by creating more or fewer partitions. It tests the efficiency of data shuffle in the streaming frameworks.
    
3. Stateful Wordcount (wordcount)

    This workload counts words cumulatively received from Kafka every few seconds. This tests the stateful operator performance and Checkpoint/Acker cost in the streaming frameworks.
    
4. Fixwindow (fixwindow)

    The workloads performs a window based aggregation. It tests the performance of window operation in the streaming frameworks.
  
    
### Supported Hadoop/Spark/Flink/Storm/Gearpump releases: ###

  - Hadoop: Apache Hadoop 2.x, CDH5, HDP
  - Spark: Spark 1.6.x, Spark 2.0.x, Spark 2.1.x, Spark 2.2.x, Spark 2.3.x, Spark 2.4.x
  - Flink: 1.0.3
  - Storm: 1.0.1
  - Gearpump: 0.8.1
  - Kafka: 0.8.2.2

---


