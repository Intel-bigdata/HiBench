package com.intel.hibench.streambench.spark.microbench

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait SpoutTops {
  def processStreamData(lines:DStream[String],ssc:StreamingContext){

  }
}
