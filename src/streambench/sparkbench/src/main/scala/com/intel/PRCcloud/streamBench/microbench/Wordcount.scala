package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import com.intel.PRCcloud.streamBench.util.BenchLogUtil
import scala.collection.mutable.Map

class Wordcount(subClassParams:ParamEntity,separator:String)
  extends RunBenchJobWithInit(subClassParams){
  var wordmap = Map[String,Long]()

  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val sep=separator
    val debug=subClassParams.debug

    val wordcount = lines.flatMap(x=>x.split(sep)).map((_,1)).reduceByKey(_+_)

    wordcount.foreachRDD(rdd => {
      rdd.collect().foreach { case (word, count) =>
        wordmap(word) = if (wordmap.contains(word)) wordmap(word) + count else count
      }
    })

    if(debug) {
      BenchLogUtil.logMsg(wordmap.mkString("\n"))
    }
  }
}
