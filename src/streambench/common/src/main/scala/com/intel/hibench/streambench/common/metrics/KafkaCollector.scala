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
import kafka.common.TopicAndPartition
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient



class KafkaCollector(zkConnect: String, metricsTopic: String,
                     outputDir: String, sampleNumber: Int, desiredThreadNum: Int) extends LatencyCollector {

  private val histogram = new Histogram(new UniformReservoir(sampleNumber))

  def start(): Unit = {

    val topicPartitions = getTopicAndPartitions(List(metricsTopic), zkConnect)

    val maxThreadNum = topicPartitions.length
    val threadNum = if (desiredThreadNum > maxThreadNum) {
      println(s"More threads than kafka partitions, throttled to $maxThreadNum")
      maxThreadNum
    } else {
      desiredThreadNum
    }

    val threads = topicPartitions.indices.groupBy(_ % threadNum).map{
      case (_, indices) =>
        new FetchThread(zkConnect, indices.map(topicPartitions), histogram)}

    println("start collecting metrics from kafka topic " + metricsTopic)

    threads.foreach(_.start)
    threads.foreach(_.join())

    val minTime = threads.map(_.getLatencyHistogram.getMinTime).min
    val maxTime = threads.map(_.getLatencyHistogram.getMaxTime).max
    val count = threads.map(_.getLatencyHistogram.getCount).sum

    report(minTime, maxTime, count)
  }

  private def getTopicAndPartitions(topics: List[String], zkConnect: String): Array[TopicAndPartition] = {
    val zkClient = new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer)
    try {
      ZkUtils.getPartitionsForTopics(zkClient, topics).flatMap {
        case (topic, partitions) => partitions.map(TopicAndPartition(topic, _))
      }.toArray
    } catch {
      case e: Exception =>
        throw e
    } finally {
      zkClient.close()
    }
  }


  private def report(minTime: Long, maxTime: Long, count: Long): Unit = {
    val outputFile = new File(outputDir, metricsTopic + ".csv")
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


