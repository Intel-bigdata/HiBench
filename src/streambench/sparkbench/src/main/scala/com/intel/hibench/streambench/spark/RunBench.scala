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

package com.intel.hibench.streambench.spark

import com.intel.hibench.common.HiBenchConfig
import com.intel.hibench.streambench.common.metrics.{KafkaReporter, MetricsUtil, LatencyReporter}
import com.intel.hibench.streambench.common.{Platform, ConfigLoader, StreamBenchConfig}
import com.intel.hibench.streambench.spark.util.SparkBenchConfig
import com.intel.hibench.streambench.spark.application._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * The entry point of Spark Streaming benchmark
  */
object RunBench {

  def main(args: Array[String]) {
    val conf = new ConfigLoader(args(0))

    // Load configuration
    val master = conf.getProperty(HiBenchConfig.SPARK_MASTER)
    val reportDir = conf.getProperty(HiBenchConfig.REPORT_DIR)

    val batchInterval = conf.getProperty(StreamBenchConfig.SPARK_BATCH_INTERVAL).toInt
    val receiverNumber = conf.getProperty(StreamBenchConfig.SPARK_RECEIVER_NUMBER).toInt
    val copies = conf.getProperty(StreamBenchConfig.SPARK_STORAGE_LEVEL).toInt
    val enableWAL = conf.getProperty(StreamBenchConfig.SPARK_ENABLE_WAL).toBoolean
    val checkPointPath = conf.getProperty(StreamBenchConfig.SPARK_CHECKPOINT_PATHL)
    val directMode = conf.getProperty(StreamBenchConfig.SPARK_USE_DIRECT_MODE).toBoolean
    val benchName = conf.getProperty(StreamBenchConfig.TESTCASE)
    val topic = conf.getProperty(StreamBenchConfig.KAFKA_TOPIC)
    val zkHost = conf.getProperty(StreamBenchConfig.ZK_HOST)
    val consumerGroup = conf.getProperty(StreamBenchConfig.CONSUMER_GROUP)
    val brokerList = if (directMode) conf.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST) else ""
    val debugMode = conf.getProperty(StreamBenchConfig.DEBUG_MODE).toBoolean
    val recordPerInterval = conf.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL).toLong
    val intervalSpan: Int = conf.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN).toInt

    // val separator = conf.getProperty(StreamBenchConfig.SEPARATOR)
    val recordCount = conf.getProperty("hibench.streamingbench.record_count").toLong
    val coreNumber = conf.getProperty("hibench.yarn.executor.num").toInt * conf.getProperty("hibench.yarn.executor.cores").toInt

    // val logPath = reportDir + "/streamingbench/spark/streambenchlog.txt"
    val reporterTopic = MetricsUtil.getTopic(Platform.SPARK, topic, recordPerInterval, intervalSpan)
    println("Reporter Topic" + reporterTopic)

    val probability = conf.getProperty(StreamBenchConfig.SAMPLE_PROBABILITY).toDouble
    // init SparkBenchConfig, it will be passed into every test case
    val config = SparkBenchConfig(master, benchName, batchInterval, receiverNumber, copies,
      enableWAL, checkPointPath, directMode, zkHost, consumerGroup, topic, reporterTopic,
      brokerList, recordCount, debugMode, coreNumber, probability)

    run(config)
  }

  private def run(config: SparkBenchConfig) {

    // select test case based on given benchName
    val testCase : BenchBase = config.benchName match {
      case "identity" => new Identity()
      case "repartition" => new Repartition()
      case "project" => new Project()
      case "wordcount" => new WordCount()
      case "statistics" =>
        // This test case will consume KMeansData instead of UserVisit as other test case
        new NumericCalc()
      case other =>
        throw new Exception(s"test case ${other} is not supported")
    }

    // defind streaming context
    val conf = new SparkConf().setMaster(config.master).setAppName(config.benchName)
    val ssc = new StreamingContext(conf, Milliseconds(config.batchInterval))
    ssc.checkpoint(config.checkpointPath)

    // add listener to collect static information.
    // val listener = new LatencyListener(ssc, config)
    // ssc.addStreamingListener(listener)

    val lines: DStream[(String, String)] = if (config.directMode) {
      // direct mode with low level Kafka API
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, config.kafkaParams, Set(config.sourceTopic))

    } else {
      // receiver mode with high level Kafka API
      val kafkaInputs = (1 to config.receiverNumber).map{ _ =>
        KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
          ssc, config.kafkaParams, Map(config.sourceTopic -> config.threadsPerReceiver), config.storageLevel)
      }
      ssc.union(kafkaInputs)
    }

    // convent key from String to Long, it stands for event creation time.
    val parsedLines = lines.map{ case (k, v) => (k.toLong, v) }
    testCase.process(parsedLines, config)

    ssc.start()
    ssc.awaitTermination()
  }
}
