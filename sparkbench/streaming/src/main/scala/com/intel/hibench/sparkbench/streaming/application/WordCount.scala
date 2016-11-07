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

package com.intel.hibench.sparkbench.streaming.application

import com.intel.hibench.common.streaming.UserVisitParser
import com.intel.hibench.common.streaming.metrics.KafkaReporter
import com.intel.hibench.sparkbench.streaming.util.SparkBenchConfig
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StateSpec, State}

class WordCount() extends BenchBase {

  override def process(lines: DStream[(Long, String)], config: SparkBenchConfig) = {
    val reportTopic = config.reporterTopic
    val brokerList = config.brokerList

    // Project Line to UserVisit, the output means "[IP, [Strat Time, Count]]"
    val parsedLine: DStream[(String, (Long, Int))] = lines.map(line => {
      val userVisit = UserVisitParser.parse(line._2)
      (userVisit.getIp, (line._1, 1))
    })

    // Define state mapping function
    val mappingFunc = (ip: String, one: Option[(Long, Int)], state: State[Int]) => {
      if (!one.isDefined) {
        throw new Exception("input value is not defined. It should not happen as we don't use timeout function.")
      }
      val sum = one.get._2 + state.getOption.getOrElse(0)
      state.update(sum)
      (ip, one.get._1)
    }


    val wordCount = parsedLine.mapWithState(StateSpec.function(mappingFunc))

    wordCount.foreachRDD(rdd => rdd.foreachPartition(partLines => {
      val reporter = new KafkaReporter(reportTopic, brokerList)
      partLines.foreach { case (word, inTime) =>
        val outTime = System.currentTimeMillis()
        reporter.report(inTime, outTime)
        if (config.debugMode) println(word + ": " + inTime + ", " + outTime )
      }
    }))
  }
}
