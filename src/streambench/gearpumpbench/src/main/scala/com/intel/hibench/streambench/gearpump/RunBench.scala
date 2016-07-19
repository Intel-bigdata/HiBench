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

import com.intel.hibench.streambench.common.metrics.{MetricsUtil, LatencyReporter, KafkaReporter}
import com.intel.hibench.streambench.common.{Platform, ConfigLoader}
import com.intel.hibench.streambench.gearpump.application._
import com.intel.hibench.streambench.gearpump.source.KafkaSourceProvider
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

    val reporter = getReporter(confLoader)
    val benchConf = UserConfig.empty
        .withValue(GearpumpConfig.BENCH_CONFIG, gearConf)
        .withValue(GearpumpConfig.BENCH_LATENCY_REPORTER, reporter)
    context.submit(benchmark.application(benchConf))
    context.close()
  }

  private def getConfig(conf: ConfigLoader): GearpumpConfig = {
    val benchName = conf.getProperty("hibench.streamingbench.benchname")
    val topic = conf.getProperty("hibench.streamingbench.topic_name")
    val zkHost = conf.getProperty("hibench.streamingbench.zookeeper.host")
    val consumerGroup = conf.getProperty("hibench.streamingbench.consumer_group")
    val kafkaPartitions = conf.getProperty("hibench.streamingbench.partitions").toInt
    val parallelism = conf.getProperty("hibench.streamingbench.gearpump.parallelism").toInt
    val recordCount = conf.getProperty("hibench.streamingbench.record_count").toLong
    val brokerList = conf.getProperty("hibench.streamingbench.brokerList")
    val pattern = conf.getProperty("hibench.streamingbench.pattern")
    val fieldIndex = conf.getProperty("hibench.streamingbench.field_index").toInt
    val separator = conf.getProperty("hibench.streamingbench.separator")
    val prob = conf.getProperty("hibench.streamingbench.prob").toDouble

    GearpumpConfig(benchName, zkHost, brokerList, consumerGroup, topic, kafkaPartitions,
      recordCount, parallelism, pattern, fieldIndex, separator, prob)
  }

  private def getReporter(conf: ConfigLoader): LatencyReporter = {
    val topic = conf.getProperty("hibench.streamingbench.topic_name")
    val brokerList = conf.getProperty("hibench.streamingbench.brokerList")
    val recordPerInterval = conf.getProperty("hibench.streamingbench.prepare.periodic.recordPerInterval").toLong
    val intervalSpan: Int = conf.getProperty("hibench.streamingbench.prepare.periodic.intervalSpan").toInt
    val reporterTopic = MetricsUtil.getTopic(Platform.GEARPUMP, topic, recordPerInterval, intervalSpan)
    new KafkaReporter(reporterTopic, brokerList)
  }
}
