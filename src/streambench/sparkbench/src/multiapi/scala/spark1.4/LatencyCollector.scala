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

package com.intel.hibench.streambench.spark.metrics

import com.intel.hibench.streambench.spark.util.SparkBenchConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._

class StopContextThread(ssc: StreamingContext) extends Runnable {
  def run {
    ssc.stop(true, true)
  }
}

class LatencyListener(ssc: StreamingContext, params: SparkBenchConfig, logger: Logger) extends StreamingListener {

  var startTime = 0L
  var endTime = 0L
  //This delay is processDelay of every batch * record count in this batch
  var totalDelay = 0L
  var hasStarted = false
  var batchCount = 0
  var totalRecords = 0L

  val thread: Thread = new Thread(new StopContextThread(ssc))

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit={
    val batchInfo = batchCompleted.batchInfo
    val prevCount = totalRecords
    var recordThisBatch = batchInfo.numRecords

    if (totalRecords < params.recordCount && !hasStarted) {
      startTime = batchCompleted.batchInfo.submissionTime
      hasStarted = true
    }

    if (hasStarted && !thread.isAlive) {
      totalRecords += recordThisBatch
      logger.logMsg("LatencyController:    this batch: " + recordThisBatch)
      logger.logMsg("LatencyController: total records: " + totalRecords)
      batchCompleted.batchInfo.processingDelay match {
        case Some(value) => {
          totalDelay += value * recordThisBatch
          val valueAdjust = value + params.batchInterval.toInt * 500
          logger.logLatency(recordThisBatch, s"$valueAdjust")
        }
        case None =>  //Nothing
      }
      batchCount += 1
    }

    if (totalRecords >= params.recordCount) {
      if (hasStarted && !thread.isAlive) {
        //not receiving any data more, finish
        endTime = System.currentTimeMillis()
        val totalTime = (endTime-startTime) / 1000
        //This is weighted avg of every batch process time. The weight is records processed int the batch
        val avgLatency = totalDelay / totalRecords
        if (avgLatency > params.batchInterval * 1000)
          logger.logMsg("WARNING:SPARK CLUSTER IN UNSTABLE STATE. TRY REDUCE INPUT SPEED")

        val recordThroughput = totalRecords / totalTime
        val avgLatencyAdjust = avgLatency + params.batchInterval.toInt * 500
        logger.logMsg("Batch count: " + batchCount)
        logger.logMsg("Record count: " + totalRecords)
        logger.logMsg("Consumed time: " + totalTime + " s")
        logger.logThroughput(s"$recordThroughput")
        logger.logAvgLatency(s"$avgLatencyAdjust")
        thread.start
      }
    }
  }
}
