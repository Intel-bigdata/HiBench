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

import com.intel.hibench.common.HiBenchConf
import com.intel.hibench.streambench.common.{ConfigLoader, Platform, StreamBenchConfig, TestCase, TempLogger}
import com.intel.hibench.streambench.spark.util.SparkBenchConfig
import com.intel.hibench.streambench.spark.application._


object RunBench {
  type LogType = TempLogger

  def main(args: Array[String]) {
    this.run(args)
  }

  def run(args: Array[String]) {
    val conf = new ConfigLoader(args(0))

    // Load configuration
    val master = conf.getProperty(HiBenchConf.SPARK_MASTER)
    val reportDir = conf.getProperty(HiBenchConf.REPORT_DIR)

    val batchInterval = conf.getProperty(StreamBenchConfig.SPARK_BATCH_INTERVAL).toInt
    val receiverNumber = conf.getProperty(StreamBenchConfig.SPARK_RECEIVER_NUMBER).toInt
    val copies = conf.getProperty(StreamBenchConfig.SPARK_STORAGE_LEVEL).toInt
    val enableWAL = conf.getProperty(StreamBenchConfig.SPARK_ENABLE_WAL).toBoolean
    val checkPointPath = conf.getProperty(StreamBenchConfig.SPARK_CHECKPOINT_PATHL)
    val directMode = conf.getProperty(StreamBenchConfig.SPARK_USE_DIRECT_MODE).toBoolean
    val benchName = conf.getProperty(StreamBenchConfig.TESTCASE)
    val topic = conf.getProperty(StreamBenchConfig.TOPIC)
    val zkHost = conf.getProperty(StreamBenchConfig.ZK_HOST)
    val consumerGroup = conf.getProperty(StreamBenchConfig.CONSUMER_GROUP)
    val brokerList = if (directMode) conf.getProperty(StreamBenchConfig.BROKER_LIST) else ""
    val debugMode = conf.getProperty(StreamBenchConfig.DEBUG_MODE).toBoolean

    // val separator = conf.getProperty(StreamBenchConfig.SEPARATOR)
    val recordCount = conf.getProperty("hibench.streamingbench.record_count").toLong
    val coreNumber = conf.getProperty("hibench.yarn.executor.num").toInt * conf.getProperty("hibench.yarn.executor.cores").toInt

    val logPath = reportDir + "/streamingbench/spark/streambenchlog.txt"

    // init SparkBenchConfig, it will be passed into every test case
    val config = SparkBenchConfig(master, benchName, batchInterval, receiverNumber, copies,
        enableWAL, checkPointPath, directMode, zkHost, consumerGroup, topic, brokerList,
        recordCount, debugMode, coreNumber)

    benchName match {
      case "identity" =>
        val logger = new LogType(logPath, Platform.Spark, TestCase.Identity)
        val identity = new Identity(config, logger)
        identity.run()
      case "project" =>
        val logger = new LogType(logPath, Platform.Spark, TestCase.Project)
        val project = new Project(config, logger)
        project.run()
      case "sample" =>
        val logger = new LogType(logPath, Platform.Spark, TestCase.Sample )
        val probability = conf.getProperty(StreamBenchConfig.SAMPLE_PROBABILITY).toDouble
        val sample = new Sample(config, logger, probability)
        sample.run()
      case "wordcount" =>
        val logger = new LogType(logPath, Platform.Spark, TestCase.WordCount)
        val wordCount = new WordCount(config, logger)
        wordCount.run()
      case "grep" =>
        val logger = new LogType(logPath, Platform.Spark, TestCase.Grep)
        val pattern = conf.getProperty(StreamBenchConfig.GREP_PATTERN)
        val grep = new Grep(config, logger, pattern)
        grep.run()
      case "distinctcount" =>
        val logger = new LogType(logPath, Platform.Spark, TestCase.DistinctCount)
        val distinctCount = new DistinctCount(config, logger)
        distinctCount.run()
      case "statistics" =>
        // This test case will consume KMeansData instead of UserVisit as other test case
        val logger = new LogType(logPath, Platform.Spark, TestCase.Statistics)
        val numericCalc = new NumericCalc(config, logger)
        numericCalc.run()
      case other =>
        System.err.println(s"test case ${other} are not supported")
        System.exit(-1)
    }
  }
}
