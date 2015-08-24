package com.intel.hibench.streambench.spark.microbench

import com.intel.hibench.streambench.spark.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import com.intel.hibench.streambench.spark.metrics.LatencyListener
import org.apache.spark.streaming.StreamingContext
import com.intel.hibench.streambench.spark.util.BenchLogUtil
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.Map

object MapPool {
  private var imap: Map[String, Long] = _
  def getMap(): Map[String, Long] = synchronized {
    if (imap == null) imap = Map()
    imap
  }
  def setMap(imap: Map[String, Long]) = synchronized {
    this.imap = imap
  }
}

class Wordcount(subClassParams:ParamEntity,separator:String)
  extends RunBenchJobWithInit(subClassParams){

  override def processStreamData(lines:DStream[String],ssc:StreamingContext){
    val sep = separator
    val wordcount = lines
      .flatMap(x => x.split(sep))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordcount.foreachRDD(rdd=> {
      rdd.foreachPartition(partitionOfRecords => {
        val imap = MapPool.getMap
        partitionOfRecords.foreach{case (word, count) =>
          imap(word) = if (imap.contains(word)) imap(word) + count else count
        }
        MapPool.setMap(imap)
      })
    })
  }
}
