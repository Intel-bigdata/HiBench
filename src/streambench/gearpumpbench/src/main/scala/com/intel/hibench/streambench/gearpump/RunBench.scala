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
package com.intel.hibench.streambench.gearpump

import com.intel.hibench.streambench.common.{ConfigLoader, Platform, TempLogger}
import com.intel.hibench.streambench.gearpump.application._
import com.intel.hibench.streambench.gearpump.metrics.MetricsReporter
import com.intel.hibench.streambench.gearpump.source.{KafkaSourceProvider, InMemorySourceProvider}
import com.intel.hibench.streambench.gearpump.util.GearpumpConfig
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext

object RunBench {
  def main(args: Array[String]) {
    this.run(args)
  }

  def run(args: Array[String]) {
    val context = ClientContext()
    implicit val system = context.system
    implicit val sourceProvider = new KafkaSourceProvider()
    val confLoader = new ConfigLoader(args(0))
    val gearConf = getConfig(confLoader)

    val benchmark = gearConf.benchName match {
      case "project" => new ProjectStream(gearConf)
      case "sample" => new SampleApp(gearConf)
      case "statistics" => new NumericCalc(gearConf)
      case "wordcount" => new WordCount(gearConf)
      case "grep" => new GrepApp(gearConf)
      case "distinctcount" => new DistinctCount(gearConf)
      case _ => new IdentityApp(gearConf)
    }

    val reportDir = confLoader.getProperty("hibench.report.dir")
    val logPath = reportDir + "/streamingbench/gearpump/streambenchlog.txt"
    val logger = new TempLogger(logPath, Platform.Gearpump, benchmark.benchName)

    val benchConf = UserConfig.empty.withValue(GearpumpConfig.BENCHCONFIG, gearConf)
    val appId = context.submit(benchmark.application(benchConf))
    val metricsReporter = new MetricsReporter(appId, context, logger, gearConf.recordCount)
    metricsReporter.reportMetrics()
    context.close()
  }

  private def getConfig(conf: ConfigLoader): GearpumpConfig = {
    val benchName = conf.getProperty("hibench.streamingbench.benchname")
    val topic = conf.getProperty("hibench.streamingbench.topic_name")
    val zkHost = conf.getProperty("hibench.streamingbench.zookeeper.host")
    val consumerGroup = conf.getProperty("hibench.streamingbench.consumer_group")
    val kafkaPartitions = conf.getProperty("hibench.streamingbench.partitions").toInt
    val parallelism = conf.getProperty("hibench.streamingbench.gearpump.parallelism").toInt
    val kafkaThreads = conf.getProperty("hibench.streamingbench.receiver_nodes").toInt
    val recordCount = conf.getProperty("hibench.streamingbench.record_count").toLong
    val copies = conf.getProperty("hibench.streamingbench.copies").toInt
    val debug = conf.getProperty("hibench.streamingbench.debug").toBoolean
    val directMode = conf.getProperty("hibench.streamingbench.direct_mode").toBoolean
    val brokerList = if (directMode) conf.getProperty("hibench.streamingbench.brokerList") else ""

    val pattern = conf.getProperty("hibench.streamingbench.pattern")
    val fieldIndex = conf.getProperty("hibench.streamingbench.field_index").toInt
    val separator = conf.getProperty("hibench.streamingbench.separator")
    val prob = conf.getProperty("hibench.streamingbench.prob").toDouble

    GearpumpConfig(benchName, zkHost, brokerList, consumerGroup, topic, kafkaPartitions,
      recordCount, parallelism, pattern, fieldIndex, separator, prob)
  }
}
