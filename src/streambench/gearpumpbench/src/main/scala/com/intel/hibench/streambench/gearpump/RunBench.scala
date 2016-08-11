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
import com.intel.hibench.streambench.common.{StreamBenchConfig, Platform, ConfigLoader}
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
    val benchName = conf.getProperty(StreamBenchConfig.TESTCASE)
    val topic = conf.getProperty(StreamBenchConfig.KAFKA_TOPIC)
    val zkHost = conf.getProperty(StreamBenchConfig.ZK_HOST)
    val consumerGroup = conf.getProperty(StreamBenchConfig.CONSUMER_GROUP)
    val parallelism = conf.getProperty(StreamBenchConfig.GEARPUMP_PARALLELISM).toInt
    val brokerList = conf.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST)
    val prob = conf.getProperty(StreamBenchConfig.SAMPLE_PROBABILITY).toDouble

    GearpumpConfig(benchName, zkHost, brokerList, consumerGroup, topic,
      parallelism, prob)
  }

  private def getReporter(conf: ConfigLoader): LatencyReporter = {
    val topic = conf.getProperty(StreamBenchConfig.KAFKA_TOPIC)
    val brokerList = conf.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST)
    val recordPerInterval = conf.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL).toLong
    val intervalSpan: Int = conf.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN).toInt
    val reporterTopic = MetricsUtil.getTopic(Platform.GEARPUMP, topic, recordPerInterval, intervalSpan)
    new KafkaReporter(reporterTopic, brokerList)
  }
}
