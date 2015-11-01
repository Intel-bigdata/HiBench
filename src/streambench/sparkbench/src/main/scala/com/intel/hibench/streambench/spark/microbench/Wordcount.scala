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

import com.intel.hibench.streambench.spark.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import com.intel.hibench.streambench.spark.metrics.LatencyListener
import org.apache.spark.streaming.StreamingContext
import com.intel.hibench.streambench.spark.util.BenchLogUtil
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.Map

object MapPool {
  private var imap: Map[String, Long] = _
  def getMap(): Map[String, Long] = synchronized {
    if (imap == null) imap = Map()
    imap
  }
  def setMap(imap: Map[String, Long]) = synchronized {
    this.imap = imap
  }
}

class Wordcount(subClassParams:ParamEntity,separator:String)
  extends RunBenchJobWithInit(subClassParams){

  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val sep = separator
    val wordcount = lines
      .flatMap(x => x.split(sep))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordcount.foreachRDD(rdd=> {
      rdd.foreachPartition(partitionOfRecords => {
        val imap = MapPool.getMap
        partitionOfRecords.foreach{case (word, count) =>
          imap(word) = if (imap.contains(word)) imap(word) + count else count
        }
        MapPool.setMap(imap)
      })
    })
  }
}
