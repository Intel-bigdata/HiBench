package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import com.intel.PRCcloud.streamBench.metrics.LatencyListener
import org.apache.spark.streaming.StreamingContext
import com.intel.PRCcloud.streamBench.util.BenchLogUtil

class NumericCalcJob(subClassParams:ParamEntity,fieldIndex:Int,separator:String)
  extends RunBenchJobWithInit(subClassParams) {

  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val index=fieldIndex
    val sep=separator

    var max:Long=0
    var min:Long=Int.MaxValue
    var sum:Long=0
    var totalCount:Long=0

    lines.foreachRDD(rdd=>{
      val numbers=rdd.flatMap(line=>{
        val splits=line.split(sep)
        if(index<splits.length)
          Iterator(splits(index).toLong)
        else
          Iterator.empty
      })


      val curMax=numbers.fold(0)((v1,v2)=>Math.max(v1, v2))
      max=Math.max(max, curMax)
      val curMin=numbers.fold(Int.MaxValue)((v1,v2)=>Math.min(v1, v2))
      min=Math.min(min, curMin)
      var curSum=numbers.fold(0)((v1,v2)=>v1+v2)
      sum+=curSum
      totalCount+=rdd.count()

      BenchLogUtil.logMsg("Current max:"+max+"  	Time:")
      BenchLogUtil.logMsg("Current min:"+min+"  	Time:")
      BenchLogUtil.logMsg("Current sum:"+sum+"  	Time:")
      BenchLogUtil.logMsg("Current total:"+totalCount+"  	Time:")
      BenchLogUtil.logMsg("Current avg:"+(sum.toDouble/totalCount.toDouble)+"  	Time:")

    })
  }
}
