package com.intel.PRCcloud.streamBench.metrics

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._
import com.intel.PRCcloud.streamBench.util._

class LatencyListener(val ssc:StreamingContext,params:ParamEntity) extends StreamingListener {

  var startTime=0L
  var endTime=0L
  //This delay is processDelay of every batch * record count in this batch
  var totalDelay=0L
  var hasStarted=false
  var batchCount=0
  var totalRecords=0L

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit={
    val batchInfo = batchCompleted.batchInfo
    val prevCount=totalRecords
    var recordThisBatch = batchInfo.numRecords

    totalRecords += recordThisBatch
    BenchLogUtil.logMsg("LatencyController: total records:" + totalRecords)

    if (totalRecords == prevCount) {
      if (hasStarted) {
        //not receiving any data more, finish
        endTime = System.currentTimeMillis()
        val totalTime = (endTime-startTime).toDouble/1000
        //This is weighted avg of every batch process time. The weight is records processed int the batch
        val avgLatency = totalDelay.toDouble/totalRecords
        if (avgLatency > params.batchInterval.toDouble*1000)
          BenchLogUtil.logMsg("WARNING:SPARK CLUSTER IN UNSTABLE STATE. TRY REDUCE INPUT SPEED")

        val avgLatencyAdjust = avgLatency + params.batchInterval.toDouble*500
        val recordThroughput = params.recordCount / totalTime
        BenchLogUtil.logMsg("Batch count = " + batchCount)
        BenchLogUtil.logMsg("Total processing delay = " + totalDelay + " ms")
        BenchLogUtil.logMsg("Consumed time = " + totalTime + " s")
        BenchLogUtil.logMsg("Avg latency/batchInterval = " + avgLatencyAdjust + " ms")
        BenchLogUtil.logMsg("Avg records/sec = " + recordThroughput + " records/s")
        ssc.stop(true,true)
        System.exit(0)
      }
    } else if (!hasStarted) {
      startTime = batchCompleted.batchInfo.submissionTime
      hasStarted = true
    }

    if (hasStarted) {
//      BenchLogUtil.logMsg("This delay:"+batchCompleted.batchInfo.processingDelay+"ms")
      batchCompleted.batchInfo.processingDelay match {
        case Some(value) => totalDelay += value*recordThisBatch
        case None =>  //Nothing
      }
      batchCount = batchCount+1
    }
  }
  
}
