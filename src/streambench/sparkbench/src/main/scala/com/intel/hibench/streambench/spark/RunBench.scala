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

import com.intel.hibench.streambench.common.{TestCase, Platform, TempLogger}
import com.intel.hibench.streambench.spark.entity.ParamEntity
import com.intel.hibench.streambench.spark.microbench._

object RunBench {
  var reportDir = ""

  def main(args: Array[String]) {
    this.run(args)
  }

  def run(args: Array[String]) {
    val conf = new ConfigLoader(args(0))

    val benchName = conf.getProperty("hibench.streamingbench.benchname")
    val topic = conf.getProperty("hibench.streamingbench.topic_name")
    val master = conf.getProperty("hibench.spark.master")
    val batchInterval = conf.getProperty("hibench.streamingbench.batch_interval").toInt
    val zkHost = conf.getProperty("hibench.streamingbench.zookeeper.host")
    val consumerGroup = conf.getProperty("hibench.streamingbench.consumer_group")
    val kafkaThreads = conf.getProperty("hibench.streamingbench.receiver_nodes").toInt
    val recordCount = conf.getProperty("hibench.streamingbench.record_count").toLong
    val copies = conf.getProperty("hibench.streamingbench.copies").toInt
    val testWAL = conf.getProperty("hibench.streamingbench.testWAL").toBoolean
    val path = if (testWAL) conf.getProperty("hibench.streamingbench.checkpoint_path") else ""
    val debug = conf.getProperty("hibench.streamingbench.debug").toBoolean
    val directMode = conf.getProperty("hibench.streamingbench.direct_mode").toBoolean
    val brokerList = if (directMode) conf.getProperty("hibench.streamingbench.brokerList") else ""
    val totalParallel = conf.getProperty("hibench.yarn.executor.num").toInt * conf.getProperty("hibench.yarn.executor.cores").toInt

    this.reportDir = conf.getProperty("hibench.report.dir")

    val logPath = reportDir + "/streamingbench/spark/streambenchlog.txt"
    val Spark = Platform.Spark

    val param = ParamEntity(master, benchName, batchInterval, zkHost, consumerGroup, topic, kafkaThreads, recordCount, copies, testWAL, path, debug, directMode, brokerList, totalParallel)
    println(s"params:$param")
    benchName match {
      case "project" =>
        val logger = new TempLogger(logPath, Platform.Spark, TestCase.Project)
        val fieldIndex = conf.getProperty("hibench.streamingbench.field_index").toInt
        val separator = conf.getProperty("hibench.streamingbench.separator")
        val ProjectTest = new StreamProjectionJob(param, fieldIndex, separator, logger)
        ProjectTest.run()
      case "sample" =>
        val logger = new TempLogger(logPath, Platform.Spark, TestCase.Sample )
        val prob = conf.getProperty("hibench.streamingbench.prob").toDouble
        val SampleTest = new SampleStreamJob(param, prob, logger)
        SampleTest.run()
      case "statistics" =>
        val logger = new TempLogger(logPath, Platform.Spark, TestCase.Statistics)
        val fieldIndex = conf.getProperty("hibench.streamingbench.field_index").toInt
        val separator = conf.getProperty("hibench.streamingbench.separator")
        val numericCalc = new NumericCalcJob(param, fieldIndex, separator, logger)
        numericCalc.run()
      case "wordcount" =>
        val logger = new TempLogger(logPath, Platform.Spark, TestCase.WordCount)
        val separator = conf.getProperty("hibench.streamingbench.separator")
        val wordCount = new Wordcount(param, separator, logger)
        wordCount.run()
      case "grep" =>
        val logger = new TempLogger(logPath, Platform.Spark, TestCase.Grep)
        val pattern = conf.getProperty("hibench.streamingbench.pattern")
        val GrepStream = new GrepStreamJob(param, pattern, logger)
        GrepStream.run()
      case "distinctcount" =>
        val logger = new TempLogger(logPath, Platform.Spark, TestCase.DistinctCount)
        val fieldIndex = conf.getProperty("hibench.streamingbench.field_index").toInt
        val separator = conf.getProperty("hibench.streamingbench.separator")
        val distinct = new DistinctCountJob(param, fieldIndex, separator, logger)
        distinct.run()
      case _ =>
        val logger = new TempLogger(logPath, Platform.Spark, TestCase.Identity)
        val emptyTest = new IdentityJob(param, logger)
        emptyTest.run()
    }
  }
}
