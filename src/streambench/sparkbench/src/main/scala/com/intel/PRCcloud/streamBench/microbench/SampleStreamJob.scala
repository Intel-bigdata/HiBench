package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import com.intel.PRCcloud.streamBench.util.BenchLogUtil
import com.intel.PRCcloud.streamBench.metrics.LatencyListener
import org.apache.spark.streaming.StreamingContext

import scala.util.Random

object ThreadLocalRandom {
  private val localRandom = new ThreadLocal[util.Random] {
    override protected def initialValue() = new util.Random
  }

  def current = localRandom.get
}

class SampleStreamJob(subClassParams:ParamEntity,probability:Double)
  extends RunBenchJobWithInit(subClassParams) {

  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val prob=probability
    val samples=lines.filter( _=> {
      ThreadLocalRandom.current.nextDouble() < prob
      //Math.random() < prob
    })
    val debug=subClassParams.debug
    if(debug){
      var totalCount=0L
      samples.foreachRDD(rdd=>{
        totalCount+=rdd.count()
        BenchLogUtil.logMsg("Current sample count:"+totalCount)
      })
    }else{
      samples.foreachRDD(rdd => rdd.foreach( _ => Unit ))
    }

  }
}
