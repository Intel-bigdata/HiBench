package com.intel.hibench.streambench.spark.microbench

import com.intel.hibench.streambench.spark.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import com.intel.hibench.streambench.spark.util.BenchLogUtil

class GrepStreamJob(subClassParams:ParamEntity,patternStr:String) extends RunBenchJobWithInit(subClassParams){
  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    BenchLogUtil.logMsg("In GrepStreamJob")
    val pattern=patternStr
    val debug=subClassParams.debug
    val matches=lines.filter(_.contains(pattern))

    if(debug){
      matches.print()
    }else{
      matches.foreachRDD( rdd => rdd.foreach( _ => Unit ))
    }
  }
}
