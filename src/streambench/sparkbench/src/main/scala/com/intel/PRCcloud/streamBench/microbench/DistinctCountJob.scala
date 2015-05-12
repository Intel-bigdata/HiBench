package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import com.intel.PRCcloud.streamBench.util.BenchLogUtil

class DistinctCountJob (subClassParams:ParamEntity,fieldIndex:Int,separator:String) extends RunBenchJobWithInit(subClassParams) {

  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val index=fieldIndex
    val sep=separator
    val debug=subClassParams.debug
    var set=Set[String]()

    lines.foreachRDD(rdd=>{
      val fields=rdd.flatMap(line=>{
        val splits=line.split(sep)
        if(index<splits.length)
          Iterator(splits(index))
        else
          Iterator.empty
      })
      val valuesArray=fields.distinct.collect
      valuesArray.foreach(value=> {
        set += value
      })
      BenchLogUtil.logMsg("set size:"+set.size)
      if(debug){
        BenchLogUtil.logMsg(set.mkString("\n"))
      }


    })
  }
}
