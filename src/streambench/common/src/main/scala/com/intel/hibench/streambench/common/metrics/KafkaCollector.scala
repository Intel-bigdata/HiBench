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

import java.io.{FileWriter, File}
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{CsvReporter, MetricRegistry}


class KafkaCollector(name: String, consumer: KafkaConsumer, outputFile: String) extends LatencyCollector {

  private var minTime = Long.MaxValue
  private var maxTime = 0L
  private var count = 0L
  private val registry = new MetricRegistry
  private val histogram = registry.histogram(name)
  private val reporter = CsvReporter.forRegistry(registry)
      .formatFor(Locale.US)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(new File(outputFile))

  def start(): Unit = {
    reporter.start(1, TimeUnit.SECONDS)

    println("start collecting metrics from kafka topic " + name)
    while (consumer.hasNext) {
      val times = new String(consumer.next(), "UTF-8").split(":")
      val startTime = times(0).toLong
      val endTime = times(1).toLong

      updateLatency(startTime, endTime)
      updateThroughput(startTime, endTime)
    }

    val throughputFile = new File(outputFile, name + ".csv")
    val outputWriter = new FileWriter(throughputFile, true)
    outputWriter.append(s"Throughput: ${count * 1.0 / (maxTime - minTime)}")
    outputWriter.close()

    val path = throughputFile.getCanonicalPath
    println(s"written out metrics to $path")


    import scala.sys.process._
    while(!Seq("tail", "-n", "1", path).!!.contains("Throughput")) {
      Thread.sleep(1000)
    }

    reporter.close()
    consumer.close()
  }

  private def updateLatency(startTime: Long, endTime: Long): Unit = {
    histogram.update(endTime - startTime)

  }

  private def updateThroughput(startTime: Long ,endTime: Long): Unit = {
    count += 1

    if (startTime < minTime) {
      minTime = startTime
    }

    if (endTime > maxTime) {
      maxTime = endTime
    }

  }

}
