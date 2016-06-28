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

import com.intel.hibench.streambench.common.{UserVisitParser, Logger}
import com.intel.hibench.streambench.spark.util.SparkBenchConfig

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StateSpec, State, StreamingContext}

class DistinctCount(config:SparkBenchConfig, logger: Logger)
  extends BenchRunnerBase(config, logger) {

  override def process(ssc: StreamingContext, lines: DStream[(Long, String)]) {
    val distinctFunc = (word: String, one: Option[Int], state: State[Int]) => {
      state.update(1)
      word
    }

    val parsedLine: DStream[(String, Int)] = lines.map(line => {
      val userVisit = UserVisitParser.parse(line._2)
      (userVisit.getIp, 1)
    })

    val distinctCount = parsedLine.mapWithState(StateSpec.function(distinctFunc)).stateSnapshots()
    if(config.debugMode) {
      distinctCount.print()
    } else {
      distinctCount.foreachRDD(rdd => rdd.foreach(_ => Unit))
    }
  }
}
