/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.sparkbench.structuredstreaming

import com.intel.hibench.common.HiBenchConfig
import com.intel.hibench.common.streaming.{TestCase, StreamBenchConfig, Platform, ConfigLoader}
import com.intel.hibench.common.streaming.metrics.MetricsUtil
import com.intel.hibench.sparkbench.structuredstreaming.util.SparkBenchConfig
import com.intel.hibench.sparkbench.structuredstreaming.application._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * The entry point of Spark Streaming benchmark
  */
object RunBench {

  def main(args: Array[String]) {
    val conf = new ConfigLoader(args(0))

    // Load configuration
    val master = conf.getProperty(HiBenchConfig.SPARK_MASTER)

    val batchInterval = conf.getProperty(StreamBenchConfig.SPARK_BATCH_INTERVAL).toInt
    val receiverNumber = conf.getProperty(StreamBenchConfig.SPARK_RECEIVER_NUMBER).toInt
    val copies = conf.getProperty(StreamBenchConfig.SPARK_STORAGE_LEVEL).toInt
    val enableWAL = conf.getProperty(StreamBenchConfig.SPARK_ENABLE_WAL).toBoolean
    val checkPointPath = conf.getProperty(StreamBenchConfig.SPARK_CHECKPOINT_PATH)
    val directMode = conf.getProperty(StreamBenchConfig.SPARK_USE_DIRECT_MODE).toBoolean
    val benchName = conf.getProperty(StreamBenchConfig.TESTCASE)
    val topic = conf.getProperty(StreamBenchConfig.KAFKA_TOPIC)
    val zkHost = conf.getProperty(StreamBenchConfig.ZK_HOST)
    val consumerGroup = conf.getProperty(StreamBenchConfig.CONSUMER_GROUP)
    val brokerList = conf.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST)
    val debugMode = conf.getProperty(StreamBenchConfig.DEBUG_MODE).toBoolean
    val recordPerInterval = conf.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL).toLong
    val intervalSpan: Int = conf.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN).toInt

    val windowDuration: Long = conf.getProperty(StreamBenchConfig.FixWINDOW_DURATION).toLong
    val windowSlideStep: Long = conf.getProperty(StreamBenchConfig.FixWINDOW_SLIDESTEP).toLong

    val coreNumber = conf.getProperty(HiBenchConfig.YARN_EXECUTOR_NUMBER).toInt * conf.getProperty(HiBenchConfig.YARN_EXECUTOR_CORES).toInt

    val producerNum = conf.getProperty(StreamBenchConfig.DATAGEN_PRODUCER_NUMBER).toInt
    val reporterTopic = MetricsUtil.getTopic(Platform.SPARK, topic, producerNum, recordPerInterval, intervalSpan)
    println("Reporter Topic: " + reporterTopic)
    val reporterTopicPartitions = conf.getProperty(StreamBenchConfig.KAFKA_TOPIC_PARTITIONS).toInt
    MetricsUtil.createTopic(zkHost, reporterTopic, reporterTopicPartitions)

    val probability = conf.getProperty(StreamBenchConfig.SAMPLE_PROBABILITY).toDouble
    // init SparkBenchConfig, it will be passed into every test case
    val config = SparkBenchConfig(master, benchName, batchInterval, receiverNumber, copies,
      enableWAL, checkPointPath, directMode, zkHost, consumerGroup, topic, reporterTopic,
      brokerList, debugMode, coreNumber, probability, windowDuration, windowSlideStep)

    runStructured(config)
  }

  private def runStructured(config: SparkBenchConfig) {
    // select test case based on given benchName
    val testCase : StructuredBenchBase = TestCase.withValue(config.benchName) match {
      case TestCase.IDENTITY => new StructuredIdentity()
      case TestCase.REPARTITION => new StructuredRepartition()
      case other =>
        throw new Exception(s"test case ${other} in structured streaming is not supported")
    }

    val ds: DataFrame = {
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.appName("structured " + config.benchName).getOrCreate()
      import spark.implicits._

      val ds1 = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafkaParams("metadata.broker.list"))
        .option("subscribe", config.sourceTopic)
        .load()

      ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    }

    testCase.process(ds, config)
  }
}
