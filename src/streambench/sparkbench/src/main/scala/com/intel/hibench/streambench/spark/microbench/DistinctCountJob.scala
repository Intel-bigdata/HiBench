package com.intel.hibench.streambench.spark.microbench

import com.intel.hibench.streambench.spark.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import com.intel.hibench.streambench.spark.util.BenchLogUtil
import org.apache.spark.streaming.StreamingContext._

import scala.collection.mutable.ArrayBuffer

object SetPool {
  private var iset: Set[String] = _
  def getSet(): Set[String] = synchronized {
    if (iset == null) {
      iset = Set()
    }
    iset
  }
  def setSet(iset: Set[String]): Unit = synchronized {
    this.iset = iset
  }
}

class DistinctCountJob (subClassParams:ParamEntity, fieldIndex:Int, separator:String) extends RunBenchJobWithInit(subClassParams) {

  override def processStreamData(lines:DStream[String], ssc:StreamingContext) {
    val index = fieldIndex
    val sep   = separator

    val distinctcount = lines
      .flatMap(line => {
      val splits = line.split(sep)
      if (index < splits.length)
        Traversable(splits(index))
      else
        Traversable.empty
    })
      .map(word => (word, 1))
      .reduceByKey((x, y) => x)
    
    distinctcount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        var iset = SetPool.getSet
        partitionOfRecords.foreach{case(word, count) =>
          iset += word
        }
        SetPool.setSet(iset)
      })
    })
  }
}
