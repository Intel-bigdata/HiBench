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
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StateSpec, State}

class WordCount() extends BenchBase {

  override def process(lines: DStream[(Long, String)], config: SparkBenchConfig) = {
    val reportTopic = config.reporterTopic
    val brokerList = config.brokerList

    val parsedLine: DStream[(String, (Int, Long))] = lines.map(line => {
      val userVisit = UserVisitParser.parse(line._2)
      (userVisit.getIp, (1, line._1))
    })

    val mappingFunc = (word: String, one: Option[(Int, Long)], state: State[Int]) => {
      if (!one.isDefined) {
        throw new Exception("input value is not defined. It should not happen as we don't use timeout function.")
      }
      val sum = one.get._1 + state.getOption.getOrElse(0)
      state.update(sum)
      (word, (sum, one.get._2))
    }

    val wordCount = parsedLine.mapWithState(StateSpec.function(mappingFunc))

    wordCount.foreachRDD(rdd => rdd.foreachPartition(partLines => {
      val reporter = new KafkaReporter(reportTopic, brokerList)
      partLines.map { case (word, (sum, inTime)) =>
        reporter.report(inTime, System.currentTimeMillis())
        if (config.debugMode) println(inTime + ", " + word + ":" + sum)
      }
    }))
  }
}
