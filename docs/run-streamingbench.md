## Overview ##
The streaming benchmark consists of the following parts:  

-  Data Generation

  The role of data generator is to generate a steady stream of data and send it to the Kafka cluster. Each record is labeled with the timestamp when it is generated.

-  Kafka cluster

  Kafka is a messaging system. Each streaming workload saves the data to its own topics. 

-  Test cluster

  This could be a Spark cluster, Flink cluster, Storm cluster or Gearpump cluster. The streaming application (identity, repartition, wordcount, fixwindow) reads data from Kafka, processes the data and writes the results back to another topic in Kafka.
  Each record in the result is also labeled a timestamp.

-  Metrics reader

  It reads the result in Kafka and calculate the time difference(Record out time - Record in time) and generate the report.

### 1. Setup ###

 * Python 2.x(>=2.6) is required.
 
 * Supported Hadoop version: Apache Hadoop 2.x, CDH5.x, HDP 
 
 * Supported Spark version: 1.6.x, 2.0.x, 2.1.x, 2.2.x 

 * Build HiBench according to [build HiBench](build-hibench.md).
 
 * Start HDFS, Yarn in the cluster.
 
 * Setup [ZooKeeper](http://zookeeper.apache.org/) (3.4.8 is preferred).

 * Setup [Apache Kafka](http://kafka.apache.org/) (0.8.2.2, scala version 2.10 is preferred).
  
 * Setup one of the streaming frameworks that you want to test.

   * [Apache Spark](http://spark.apache.org/) (1.6.1, is preferred).

   * [Apache Storm](http://storm.apache.org/) (1.0.1 is preferred).

   * [Apache Flink](http://flink.apache.org/) (1.0.3 is prefered).

   * [Apache Gearpump](http://gearpump.apache.org/) (0.8.1 is prefered)


### 2. Configure `hadoop.conf` ###

Hadoop is used to generate the input data of the workloads.
Create and edit `conf/hadoop.conf`：

    cp conf/hadoop.conf.template conf/hadoop.conf

Set the below properties properly:

Property        |      Meaning
----------------|--------------------------------------------------------
hibench.hadoop.home     |      The Hadoop installation location
hibench.hadoop.executable  |   The path of hadoop executable. For Apache Hadoop, it is /YOUR/HADOOP/HOME/bin/hadoop
hibench.hadoop.configure.dir | Hadoop configuration directory. For Apache Hadoop, it is /YOUR/HADOOP/HOME/etc/hadoop
hibench.hdfs.master       |    The root HDFS path to store HiBench data, i.e. hdfs://localhost:8020/user/username
hibench.hadoop.release    |    Hadoop release provider. Supported value: apache, cdh5, hdp

Note: For CDH and HDP users, please update `hibench.hadoop.executable`, `hibench.hadoop.configure.dir` and `hibench.hadoop.release` properly. The default value is for Apache release.


### 3. Configure Kafka ###
Set the below Kafka properites in `conf/hibench.conf` and leave others as default. 

Property        |      Meaning
----------------|--------------------------------------------------------
hibench.streambench.kafka.home     |                  /PATH/TO/YOUR/KAFKA/HOME
hibench.streambench.zkHost         | zookeeper host:port of kafka cluster, host1:port1,host2:port2...
hibench.streambench.kafka.brokerList     | Kafka broker lists, written in mode host:port,host:port,..
hibench.streambench.kafka.topicPartitions    |   Number of partitions of generated topic (default 20)

### 4. Configure Data Generator ###
Set the below Kafka properites in `conf/hibench.conf` and leave others as default. 

Param Name      | Param Meaning
----------------|--------------------------------------------------------
hibench.streambench.datagen.intervalSpan     |    Interval span in millisecond (default: 50)
hibench.streambench.datagen.recordsPerInterval   |  Number of records to generate per interval span (default: 5)
hibench.streambench.datagen.recordLength     | fixed length of record (default: 200)
hibench.streambench.datagen.producerNumber  |  Number of KafkaProducer running on different thread (default: 1)
hibench.streambench.datagen.totalRounds     | Total round count of data send (default: -1 means infinity)
hibench.streambench.datagen.totalRecords    | Number of total records that will be generated (default: -1 means infinity)

### 5. Configure the Streaming Framework ###

 * [Configure Spark Streaming](sparkstreaming-configuration.md)
 
 * [Configure Flink](flink-configuration.md)
 
 * [Configure Storm](storm-configuration.md)
 
 * [Configure Gearpump](gearpump-configuration.md)
 

### 6. Generate the data ###
Take workload `identity` as an example. `genSeedDataset.sh` generates the seed data on HDFS. `dataGen.sh` sends the data to Kafka.

    bin/workloads/streaming/identity/prepare/genSeedDataset.sh
    bin/workloads/streaming/identity/prepare/dataGen.sh

### 7. Run the streaming application ###
While the data are being sent to the Kafka, start the streaming application. Take Spark streaming as an example.

    bin/workloads/streaming/identity/spark/run.sh

### 8. Generate the report ###
`metrics_reader.sh` is used to generate the report.

    bin/workloads/streaming/identity/common/metrics_reader.sh
