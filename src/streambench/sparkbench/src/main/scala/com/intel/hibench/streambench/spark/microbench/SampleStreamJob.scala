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

object ThreadLocalRandom extends Serializable{
  private val localRandom = new ThreadLocal[util.Random] {
    override protected def initialValue() = new util.Random
  }

  def current = localRandom.get
}

class SampleStreamJob(subClassParams:ParamEntity, probability:Double, logger: Logger)
  extends RunBenchJobWithInit(subClassParams, logger) {

  override def processStreamData(lines:DStream[String], ssc:StreamingContext){
    val prob = probability
    val samples = lines.filter( _=> {
      ThreadLocalRandom.current.nextDouble() < prob
    })
    val debug = subClassParams.debug
    if(debug){
      var totalCount = 0L
      samples.foreachRDD(rdd => {
        totalCount += rdd.count()
        logger.logMsg("Current sample count:"+totalCount)
      })
    }else{
      samples.foreachRDD(rdd => rdd.foreach( _ => Unit ))
    }

  }
}
