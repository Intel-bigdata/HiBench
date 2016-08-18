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

package com.intel.hibench.streambench.spark.application

import com.intel.hibench.streambench.common.UserVisitParser
import com.intel.hibench.streambench.common.metrics.KafkaReporter
import com.intel.hibench.streambench.spark.util.SparkBenchConfig
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

class FixWindow(duration: Long, slideStep: Long) extends BenchBase {

  override def process(lines: DStream[(Long, String)], config: SparkBenchConfig): Unit = {
    val reportTopic = config.reporterTopic
    val brokerList = config.brokerList

    lines.window(Duration(duration), Duration(slideStep)).map{
      case (inTime, line) => {
        val uv = UserVisitParser.parse(line)
        (uv.getBrowser, (inTime, 1))
      }
    }.reduceByKey((value, result) => {
      val reporter = new KafkaReporter(reportTopic, brokerList)
      val outTime = System.currentTimeMillis()
      reporter.report(value._1, outTime)
      if(config.debugMode == true) {
        println("Event: " + value._1 + ", " + outTime)
      }

      // Sum
      (value._1, value._2 + result._2)
    }).foreachRDD(_ => Unit)
  }
}