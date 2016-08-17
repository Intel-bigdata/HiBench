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
import java.util.Date

import com.codahale.metrics.{UniformReservoir, Histogram}


class KafkaCollector(name: String, consumer: KafkaConsumer,
                     outputDir: String, sampleNumber: Int) extends LatencyCollector {

  private var minTime = Long.MaxValue
  private var maxTime = 0L
  private var count = 0L
  private val histogram = new Histogram(new UniformReservoir(sampleNumber))

  def start(): Unit = {

    println("start collecting metrics from kafka topic " + name)
    while (consumer.hasNext) {
      val times = new String(consumer.next(), "UTF-8").split(":")
      val startTime = times(0).toLong
      val endTime = times(1).toLong

      updateLatency(startTime, endTime)
      updateThroughput(startTime, endTime)
    }

    consumer.close()
    report()
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


  private def report(): Unit = {
    val outputFile = new File(outputDir, name + ".csv")
    println(s"written out metrics to ${outputFile.getCanonicalPath}")
    val header = "time,count,throughput(msgs/s),max_latency(ms),mean_latency(ms),min_latency(ms)," +
        "stddev_latency(ms),p50_latency(ms),p75_latency(ms),p95_latency(ms),p98_latency(ms)," +
        "p99_latency(ms),p999_latency(ms)\n"
    val fileExists = outputFile.exists()
    if (!fileExists) {
      val parent = outputFile.getParentFile
      if (!parent.exists()) {
        parent.mkdirs()
      }
      outputFile.createNewFile()
    }
    val outputFileWriter = new FileWriter(outputFile, true)
    if (!fileExists) {
      outputFileWriter.append(header)
    }
    val time = new Date(System.currentTimeMillis()).toString
    val count = histogram.getCount
    val snapshot = histogram.getSnapshot
    val throughput = count * 1000 / (maxTime - minTime)
    outputFileWriter.append(s"$time,$count,$throughput," +
        s"${formatDouble(snapshot.getMax)}," +
        s"${formatDouble(snapshot.getMean)}," +
        s"${formatDouble(snapshot.getMin)}," +
        s"${formatDouble(snapshot.getStdDev)}," +
        s"${formatDouble(snapshot.getMedian)}," +
        s"${formatDouble(snapshot.get75thPercentile())}," +
        s"${formatDouble(snapshot.get95thPercentile())}," +
        s"${formatDouble(snapshot.get98thPercentile())}," +
        s"${formatDouble(snapshot.get99thPercentile())}," +
        s"${formatDouble(snapshot.get999thPercentile())}\n")
    outputFileWriter.close()
  }

  private def formatDouble(d: Double): String = {
    "%.3f".format(d)
  }

}
