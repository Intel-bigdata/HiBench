package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import com.intel.PRCcloud.streamBench.util.BenchLogUtil

import scala.collection.mutable.ArrayBuffer

class DistinctCountJob (subClassParams:ParamEntity, fieldIndex:Int, separator:String) extends RunBenchJobWithInit(subClassParams) {
  var summed_words:Long = 0

  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val index = fieldIndex
    val sep   = separator
    val debug = subClassParams.debug

    lines.foreachRDD(rdd => {
      val distincted_words = rdd.flatMap(line => {
        val splits = line.split(sep)
        if (index < splits.length)
          Iterator(splits(index))
        else
          Iterator.empty
      }).distinct()
      val size_of_this_batch = distincted_words.count()
      summed_words += size_of_this_batch
    })
    BenchLogUtil.logMsg(s"total size:$summed_words")
    if (debug) {
      val arr = new ArrayBuffer[String]()
      BenchLogUtil.logMsg("Debug disabled due to optimization of this workload")
      //BenchLogUtil.logMsg(arr.mkString("\n"))
    }
  }
}
