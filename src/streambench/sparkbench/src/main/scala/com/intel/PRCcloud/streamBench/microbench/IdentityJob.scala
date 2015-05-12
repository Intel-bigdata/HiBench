package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import com.intel.PRCcloud.streamBench.metrics.LatencyListener
import org.apache.spark.streaming.StreamingContext
import com.intel.PRCcloud.streamBench.util.BenchLogUtil

class IdentityJob(subClassParams:ParamEntity) extends RunBenchJobWithInit(subClassParams) {

  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    lines.foreachRDD(rdd => rdd.foreach( _ => Unit ))
  }
}
