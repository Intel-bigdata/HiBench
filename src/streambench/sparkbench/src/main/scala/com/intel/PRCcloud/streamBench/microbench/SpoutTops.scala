package com.intel.PRCcloud.streamBench.microbench

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait SpoutTops {
  def processStreamData(lines:DStream[String],ssc:StreamingContext){

  }
}
