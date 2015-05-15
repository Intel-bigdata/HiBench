package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}

import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import com.intel.PRCcloud.streamBench.metrics._
import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder


class RunBenchJobWithInit(params:ParamEntity){

  def run(){
    val conf = new SparkConf().setMaster(params.master)
      .setAppName(params.appName)
      .set("spark.cleaner.ttl", "7200")
      .set("spark.executor.memory","100g")

    var ssc:StreamingContext=null

    if (!params.testWAL) {
      ssc = new StreamingContext(conf,Seconds(params.batchInterval))
    } else {
      val create = ()=> new StreamingContext(conf, Seconds(params.batchInterval))
      ssc = StreamingContext.getOrCreate(params.path, create)
      ssc.checkpoint(params.path)
    }

    val listener = new LatencyListener(ssc,params)
    ssc.addStreamingListener(listener)

    var lines:DStream[String] = null
    if (params.directMode)
      lines = createDirectStream(ssc).map(_._2)
    else
      lines = createStream(ssc).map(_._2)

    processStreamData(lines, ssc)

    ssc.start()
    ssc.awaitTermination()
  }

  def processStreamData(lines:DStream[String],ssc:StreamingContext){

  }

  def createStream(ssc:StreamingContext):DStream[(String,String)]={
    val kafkaParams=Map(
      "zookeeper.connect" -> params.zkHost,
      "group.id" -> params.consumerGroup,
      "rebalance.backoff.ms" -> "20000",
      "zookeeper.session.timeout.ms" -> "20000"
    )

    var storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    if (params.copies == 1)
      storageLevel = StorageLevel.MEMORY_ONLY

    val kafkaInputs = (1 to params.threads).map{_ =>
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,
        Map(params.topic->1), storageLevel)
    }
    ssc.union(kafkaInputs)
  }

  def createDirectStream(ssc:StreamingContext):DStream[(String, String)]={
    val kafkaParams = Map(
      "metadata.broker.list" -> params.brokerList,
      "auto.offset.reset" -> "smallest",
      "socket.receive.buffer.size" -> "1024*1024*1024"
    )

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(params.topic))
  }

}
