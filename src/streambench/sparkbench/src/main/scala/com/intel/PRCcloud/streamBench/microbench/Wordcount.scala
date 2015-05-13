package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import com.intel.PRCcloud.streamBench.metrics.LatencyListener
import org.apache.spark.streaming.StreamingContext
import com.intel.PRCcloud.streamBench.util.BenchLogUtil
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.Map

class Wordcount(subClassParams:ParamEntity,separator:String)
  extends RunBenchJobWithInit(subClassParams){
  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val sep=separator
    val debug=subClassParams.debug
    var wordmap=Map[String,Long]()


    lines.flatMap(x=>x.split(sep)).map((_,1)).reduceByKey(_+_)

    lines.foreachRDD(rdd=>{
      val words=rdd.flatMap(line=>line.split(sep))
      val pairs=words.map(word=>(word,1L))
      val wordsCounts=pairs.reduceByKey(_+_)
      val resultOfThisRound=wordsCounts.collect()
      resultOfThisRound.foreach(tuple=>{
        if(wordmap.contains(tuple._1)){
          wordmap(tuple._1)=wordmap(tuple._1)+tuple._2
        }else{
          wordmap(tuple._1)=tuple._2
        }
      })

      if(debug){
        BenchLogUtil.logMsg(wordmap.mkString("\n"))
      }
    })
  }
}
