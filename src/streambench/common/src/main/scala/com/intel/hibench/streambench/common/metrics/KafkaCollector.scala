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

package com.intel.hibench.streambench.common.metrics

import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, CsvReporter, MetricRegistry}
import org.apache.logging.log4j.core.util.FileUtils


class KafkaCollector(name: String, consumer: KafkaConsumer, outputFile: String) extends LatencyCollector {

  def start(): Unit = {
    val registry = new MetricRegistry
    val histogram = registry.histogram(name)
    val reporter = CsvReporter.forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build(new File(outputFile))
    reporter.start(1, TimeUnit.SECONDS)
    while (consumer.hasNext) {
      val latency = new String(consumer.next(), "UTF-8").toLong
      histogram.update(latency)
    }
    // wait for metrics to write out
    Thread.sleep(10000)
    reporter.close()
    consumer.close()
  }

}
