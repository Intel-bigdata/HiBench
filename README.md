# HiBench Suite [![Build Status](https://travis-ci.org/intel-hadoop/HiBench.svg?branch=master)](https://travis-ci.org/intel-hadoop/HiBench)
## The bigdata micro benchmark suite ##


- Contents:
  1. Overview
  2. Getting Started
  3. Advanced Configuration
  4. Possible Issues

---
### OVERVIEW ###

This benchmark suite contains 10 typical micro workloads. This benchmark suite also has options for users to enable input/output compression for most workloads with default compression codec (zlib). Some initial work based on this benchmark suite please refer to the included ICDE workshop paper (i.e., WISS10_conf_full_011.pdf).

Note:
 1. Since HiBench-2.2, the input data of benchmarks are all automatically generated by their corresponding prepare scripts.
 2. Since HiBench-3.0, it introduces Yarn support
 3. Since HiBench-4.0, it consists of more workload implementations on both Hadoop MR and Spark. For Spark, three different APIs including Scala, Java, Python are supportive.
 4. Since HiBench-5.0, it introduces Streaming related workloads including 4 frameworks: SparkStreaming, Storm, Storm-Trident and Samza.

  **Job based Micro benchmarks:**

  For job based tasks (in the contrast to streaming based tasks), HiBench provide following workloads:

1. Sort (sort)

    This workload sorts its *text* input data, which is generated using RandomTextWriter.

2. WordCount (wordcount)

    This workload counts the occurrence of each word in the input data, which are generated using RandomTextWriter. It is representative of another typical class of real world MapReduce jobs - extracting a small amount of interesting data from large data set.

3. TeraSort (terasort)

    TeraSort is a standard benchmark created by Jim Gray. Its input data is generated by Hadoop TeraGen example program.

4. Sleep (sleep)

    This workload sleep an amount of seconds in each task to test framework scheduler.

**SQL:**

5. Scan (scan), Join(join), Aggregate(aggregation)

    This workload is developed based on SIGMOD 09 paper "A Comparison of Approaches to Large-Scale Data Analysis" and HIVE-396. It contains Hive queries (Aggregation and Join) performing the typical OLAP queries described in the paper. Its input is also automatically generated Web data with hyperlinks following the Zipfian distribution.

**Web Search Benchmarks:**

6. PageRank (pagerank)

    This workload benchmarks PageRank algorithm implemented in Spark-MLLib/Hadoop (a search engine ranking benchmark included in pegasus 2.0) examples. The data source is generated from Web data whose hyperlinks follow the Zipfian distribution.

7. Nutch indexing (nutchindexing)

    Large-scale search indexing is one of the most significant uses of MapReduce. This workload tests the indexing sub-system in Nutch, a popular open source (Apache project) search engine. The workload uses the automatically generated Web data whose hyperlinks and words both follow the Zipfian distribution with corresponding parameters. The dict used to generate the Web page texts is the default linux dict file /usr/share/dict/linux.words.

**Machine Learning:**

9. Bayesian Classification (bayes)

    This workload benchmarks NaiveBayesian Classification implemented in Spark-MLLib/Mahout examples.

    Large-scale machine learning is another important use of MapReduce. This workload tests the Naive Bayesian (a popular classification algorithm for knowledge discovery and data mining)  trainer in Mahout 0.7, which is an open source (Apache project) machine learning library. The workload uses the automatically generated documents whose words follow the zipfian distribution. The dict used for text generation is also from the default linux file /usr/share/dict/linux.words.

10. K-means clustering (kmeans)

    This workload tests the K-means (a well-known clustering algorithm for knowledge discovery and data mining) clustering in Mahout 0.7/Spark-MLlib. The input data set is generated by GenKMeansDataset based on Uniform Distribution and Guassian Distribution.

**HDFS Benchmarks:**

11. enhanced DFSIO (dfsioe)

    Enhanced DFSIO tests the HDFS throughput of the Hadoop cluster by generating a large number of tasks performing writes and reads simultaneously. It measures the average I/O rate of each map task, the average throughput of each map task, and the aggregated throughput of HDFS cluster. Note: this benchmark doesn't have Spark corresponding implementation.

**Streaming based Micro benchmarks:**

12. Streaming (streamingbench)

  Starting from HiBench 5.0, we provide following streaming workloads for SparkStreaming, Storm, Storm-Trident and Samza:

  Benchmark | Data type | Complexity | Store state involvment
  ----------|-----------|------------|-----------------------
  Identity  | Text      | Single Step| Not Involved
  Sample    | Text      | Single Step| Not Involved
  Project   | Text      | Single Step| Not Involved
  Grep      | Text      | Single Step| Not Involved
  Wordcount | Text      | Multi Step |  Involved
  Distinctcount| Text   | Multi Step |  Involved
  Statistics| Numeric   | Multi Step |  Involved

  a) Data type
  
  Big data benchmarks can be roughly classified into two types, Textual and Numeric. Text data is converted from the data source in SQL related benchmarks, which is user visit logs generated by Zipfian  distribution. Numeric data is converted from vectors in Kmeans data samples.

  b) Complexity

  Some basic opertions are essential for understanding in any data stream computation frameworks, including identity, sample, project and grep. And multi-step operations like wordcount, distinctcount and statistics are considered for sophisticated applications.
  
  c) Store state

  One feature of stream processing is integrating stored and streaming data, which may require referring to historical information and may result in updating global status either in disk or in memory. wordcount, distinctcount and statistics are provied for a demonstration for such reqirement.
    
**Supported hadoop/spark/storm/samza release:**

  - Apache release of Hadoop 1.x and Hadoop 2.x
  - CDH4/CDH5 release of MR1 and MR2.
  - Spark1.2 - 1.6
  - Storm 0.9.3
  - Samza 0.8.0

---
### Getting Started ###
1. [Getting Started](https://github.com/intel-hadoop/HiBench/wiki/Getting-Started)
2. [Getting Started for StreamingBench](https://github.com/intel-hadoop/HiBench/wiki/Getting-Started-for-StreamingBench)

### [Advanced Configurations](https://github.com/intel-hadoop/HiBench/wiki/Advanced-Configurations) ###
### [Possible issues](https://github.com/intel-hadoop/HiBench/wiki/Possible-issues) ###
