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

import com.intel.hibench.streambench.common.KMeansDataParser
import com.intel.hibench.streambench.spark.util.SparkBenchConfig
import org.apache.spark.streaming.dstream.DStream

case class MultiReducer(val max: Double, val min: Double, val sum: Double, val count: Long) {
  def this() = this(0, Int.MaxValue, 0, 0)

  def reduceValue(value: Double): MultiReducer = {
    val max = Math.max(this.max, value)
    val min = Math.min(this.min, value)
    val sum = this.sum + value
    MultiReducer(max, min, sum, count +1)
  }

  def reduce(that: MultiReducer): MultiReducer = {
    val max = Math.max(this.max, that.max)
    val min = Math.min(this.min, that.min)
    val sum = this.sum + that.sum
    val count = this.count + that.count
    MultiReducer(max, min, sum, count)
  }
}

// TODO: Rework this test case and apply KafkaReporter
class NumericCalc() extends BenchBase {

  var history_statistics: MultiReducer = new MultiReducer()

  override def process(lines: DStream[(Long, String)], config: SparkBenchConfig): Unit = {
    val reducers = lines.map{ case (time, line) =>
      val data = KMeansDataParser.parse(line).getData
      val value = data(0)
      MultiReducer(value, value, value, 1)
    }

    reducers.foreachRDD(rdd => {
      val zero = new MultiReducer()
      val currentReducer = rdd.fold(zero)((left, right) => left.reduce(right))
      history_statistics = history_statistics.reduce(currentReducer)

      println("Current max: " + currentReducer.max)
      println("Current min: " + currentReducer.min)
      println("Current sum: " + currentReducer.sum)
      println("Current total: " + currentReducer.count)
    })
  }
}
