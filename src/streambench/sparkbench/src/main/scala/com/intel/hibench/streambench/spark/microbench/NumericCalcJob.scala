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

package com.intel.hibench.streambench.spark.microbench

import com.intel.hibench.streambench.common.Logger
import com.intel.hibench.streambench.spark.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext

case class MultiReducer(var max: Long, var min: Long, var sum: Long, var count: Long) extends Serializable {
  def this() = this(0, Int.MaxValue, 0, 0)

  def reduceValue(value: Long): MultiReducer = {
    this.max = Math.max(this.max, value)
    this.min = Math.min(this.min, value)
    this.sum += value
    this.count += 1
    this
  }

  def reduce(that: MultiReducer): MultiReducer = {
    this.max = Math.max(this.max, that.max)
    this.min = Math.min(this.min, that.min)
    this.sum += that.sum
    this.count += that.count
    this
  }
}

class NumericCalcJob(subClassParams: ParamEntity, fieldIndex: Int, separator: String, logger: Logger)
  extends RunBenchJobWithInit(subClassParams, logger) {

  var history_statistics = new MultiReducer()

  override def processStreamData(lines: DStream[String], ssc: StreamingContext) {
    val index = fieldIndex
    val sep = separator

    lines.foreachRDD( rdd => {
      val numbers = rdd.flatMap( line => {
        val splits = line.trim.split(sep)
        if (index < splits.length)
          Iterator(splits(index).toLong)
        else
          Iterator.empty
      })

      var zero = new MultiReducer()
      val cur = numbers.map(x => new MultiReducer(x, x, x, 1))
        .fold(zero)((v1, v2) => v1.reduce(v2))
      //var cur = numbers.aggregate(zero)((v, x) => v.reduceValue(x), (v1, v2) => v1.reduce(v2))
      history_statistics.reduce(cur)

      logger.logMsg("Current max: " + history_statistics.max)
      logger.logMsg("Current min: " + history_statistics.min)
      logger.logMsg("Current sum: " + history_statistics.sum)
      logger.logMsg("Current total: " + history_statistics.count)

    })
  }
}
