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

import java.nio.channels.ClosedByInterruptException

import com.codahale.metrics.Histogram
import kafka.common.TopicAndPartition

class LatencyHistogram(histogram: Histogram) {

  private var minTime = Long.MaxValue
  private var maxTime = 0L
  private var count = 0L

  def updateLatency(startTime: Long, endTime: Long): Unit = {
    histogram.update(endTime - startTime)
  }

  def updateThroughput(startTime: Long ,endTime: Long): Unit = {
    count += 1

    if (startTime < minTime) {
      minTime = startTime
    }

    if (endTime > maxTime) {
      maxTime = endTime
    }
  }

  def getMinTime: Long = minTime

  def getMaxTime: Long = maxTime

  def getCount: Long = count
}

class FetchThread(zkConnect: String,
                  topicAndPartitions: IndexedSeq[TopicAndPartition],
                  histogram: Histogram) extends Thread {

  private val latencyHistogram = new LatencyHistogram(histogram)
  private val consumers: Map[TopicAndPartition, KafkaConsumer] = createAllConsumers

  override def run(): Unit = {
    try {
      while (!Thread.currentThread().isInterrupted && fetchMessage) {
      }
    } catch {
      case e: InterruptedException => throw e
      case e: ClosedByInterruptException =>  throw e
    } finally {
      consumers.values.foreach(_.close())
    }
  }


  def getLatencyHistogram: LatencyHistogram = latencyHistogram

  private def fetchMessage: Boolean = {
    consumers.foldLeft(false) { (hasNext, tpAndConsumer) =>
      val (_, consumer) = tpAndConsumer
      if (consumer.hasNext) {
        val times = new String(consumer.next(), "UTF-8")
        val startTime = times(0).toLong
        val endTime = times(1).toLong
        latencyHistogram.updateLatency(startTime, endTime)
        latencyHistogram.updateThroughput(startTime, endTime)
        true
      } else {
        hasNext
      }
    }
  }

  private def createAllConsumers: Map[TopicAndPartition, KafkaConsumer] = {
    topicAndPartitions.map(tp => tp -> new KafkaConsumer(zkConnect, tp.topic, tp.partition)).toMap
  }

}
