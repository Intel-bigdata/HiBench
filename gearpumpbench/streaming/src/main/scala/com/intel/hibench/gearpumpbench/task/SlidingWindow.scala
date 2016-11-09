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

package com.intel.hibench.gearpumpbench.task

import com.intel.hibench.common.streaming.metrics.KafkaReporter
import com.intel.hibench.gearpumpbench.util.GearpumpConfig
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.eclipse.collections.api.block.procedure.Procedure
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.eclipse.collections.impl.map.sorted.mutable.TreeSortedMap

import scala.collection.mutable.ArrayBuffer

class SlidingWindow(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private val benchConfig = conf.getValue[GearpumpConfig](GearpumpConfig.BENCH_CONFIG).get
  private val windowDuration = benchConfig.windowDuration
  private val windowStep = benchConfig.windowSlideStep
  val reporter = new KafkaReporter(benchConfig.reporterTopic, benchConfig.brokerList)

  // windowStartTime -> (ip -> (minMessageTime, count))
  private val windowCounts = new TreeSortedMap[Long, UnifiedMap[String, (TimeStamp, Long)]]

  override def onNext(message: Message): Unit = {
    val ip = message.msg.asInstanceOf[String]
    val msgTime = System.currentTimeMillis()
    getWindows(msgTime).foreach { window =>
      val countsByIp = if (windowCounts.containsKey(window)) {
        windowCounts.get(window)
      } else {
        new UnifiedMap[String, (TimeStamp, Long)]
      }
      val (minTime, count) = if (countsByIp.containsKey(ip)) {
        countsByIp.get(ip)
      } else {
        (msgTime, 0L)
      }
      countsByIp.put(ip, (Math.min(msgTime, minTime), count + 1L))
      windowCounts.put(window, countsByIp)
    }

    var hasNext = true
    while (hasNext && !windowCounts.isEmpty) {
      val windowStart = windowCounts.firstKey()
      if (msgTime >= (windowStart + windowDuration)) {
        val countsByIp = windowCounts.remove(windowStart)
        countsByIp.forEachValue(new Procedure[(TimeStamp, Long)]() {
          override def value(tuple: (TimeStamp, Long)): Unit = {
            (1 to tuple._2.toInt).foreach(i => reporter.report(tuple._1, msgTime))
          }
        })
      } else {
        hasNext = false
      }
    }
  }

  private def getWindows(timestamp: TimeStamp): List[TimeStamp] = {
    val windows = ArrayBuffer.empty[TimeStamp]
    var start = lastStartFor(timestamp)
    windows += start
    start -= windowStep
    while (start >= timestamp) {
      windows += start
      start -= windowStep
    }
    windows.toList
  }

  private def lastStartFor(timestamp: TimeStamp): TimeStamp = {
    timestamp - (timestamp + windowStep) % windowStep
  }
}

