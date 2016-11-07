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

package com.intel.hibench.common.streaming.metrics

import java.util.concurrent.Callable

import com.codahale.metrics.Histogram

class FetchJob(zkConnect: String, topic: String, partition: Int,
    histogram: Histogram) extends Callable[FetchJobResult] {

  override def call(): FetchJobResult = {
    val result = new FetchJobResult()
    val consumer = new KafkaConsumer(zkConnect, topic, partition)
    while (consumer.hasNext) {
      val times = new String(consumer.next(), "UTF-8").split(":")
      val startTime = times(0).toLong
      val endTime = times(1).toLong
      // correct negative value which might be caused by difference of system time
      histogram.update(Math.max(0, endTime - startTime))
      result.update(startTime, endTime)
    }
    println(s"Collected ${result.count} results for partition: ${partition}")
    result
  }
}

class FetchJobResult(var minTime: Long, var maxTime: Long, var count: Long) {

  def this() = this(Long.MaxValue, Long.MinValue, 0)

  def update(startTime: Long ,endTime: Long): Unit = {
    count += 1

    if(startTime < minTime) {
      minTime = startTime
    }

    if(endTime > maxTime) {
      maxTime = endTime
    }
  }
}
