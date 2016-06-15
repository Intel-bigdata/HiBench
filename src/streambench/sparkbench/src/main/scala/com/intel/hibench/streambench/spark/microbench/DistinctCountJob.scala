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

object SetPool {
  private var iset: Set[String] = _
  def getSet(): Set[String] = synchronized {
    if (iset == null) {
      iset = Set()
    }
    iset
  }
  def setSet(iset: Set[String]): Unit = synchronized {
    this.iset = iset
  }
}

class DistinctCountJob (subClassParams:ParamEntity, fieldIndex:Int, separator:String, logger: Logger)
  extends RunBenchJobWithInit(subClassParams, logger) {

  override def processStreamData(lines:DStream[String], ssc:StreamingContext) {
    val index = fieldIndex
    val sep   = separator

    val distinctcount = lines
      .flatMap(line => {
      val splits = line.split(sep)
      if (index < splits.length)
        Traversable(splits(index))
      else
        Traversable.empty
    })
      .map(word => (word, 1))
      .reduceByKey((x, y) => x)
    
    distinctcount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        var iset = SetPool.getSet
        partitionOfRecords.foreach{case(word, count) =>
          iset += word
        }
        SetPool.setSet(iset)
      })
    })
  }
}
