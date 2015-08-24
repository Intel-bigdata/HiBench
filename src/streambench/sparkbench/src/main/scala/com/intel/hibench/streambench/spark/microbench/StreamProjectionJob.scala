package com.intel.hibench.streambench.spark.microbench

import com.intel.hibench.streambench.spark.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import com.intel.hibench.streambench.spark.util.BenchLogUtil

class StreamProjectionJob(subClassParams:ParamEntity,fieldIndex:Int,separator:String)
  extends RunBenchJobWithInit(subClassParams) {
  
  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val sep   = separator
    val index = fieldIndex
    val debug = subClassParams.debug
    lines.foreachRDD(rdd => {
      val fields = rdd.flatMap(line => {
        val splits = line.trim.split(sep)
        if(index < splits.length)
          Iterator(splits(index))
        else
          Iterator.empty
      })
      fields.foreach(rdd => rdd.foreach( _ => Unit ))
      if(debug)
        BenchLogUtil.logMsg(fields.collect().mkString("\n"))
    })
  }

}
