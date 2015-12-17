/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.streambench.spark.microbench

import com.intel.hibench.streambench.spark.entity.ParamEntity
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}

import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import com.intel.hibench.streambench.spark.metrics._
import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder

class RunBenchJobWithInit(params:ParamEntity) extends SpoutTops {

  def run(){
    val conf = new SparkConf().setMaster(params.master)
      .setAppName(params.appName)
      .set("spark.cleaner.ttl", "7200")

    var ssc:StreamingContext=null

    if (!params.testWAL) {
      ssc = new StreamingContext(conf, Seconds(params.batchInterval))
    } else {
      val create = ()=> new StreamingContext(conf, Seconds(params.batchInterval))
      ssc = StreamingContext.getOrCreate(params.path, create)
      ssc.checkpoint(params.path)
    }

    val thread = new Thread(new StopContextThread(ssc))
    val listener = new LatencyListener(ssc, params, thread)
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

  def createStream(ssc:StreamingContext):DStream[(String, String)] = {
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
      println(s"Create kafka input, args:$kafkaParams")
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,
        Map(params.topic -> params.totalParallel / params.threads), storageLevel)
    }

    ssc.union(kafkaInputs)
  }

  def createDirectStream(ssc:StreamingContext):DStream[(String, String)]={
    val kafkaParams = Map(
      "metadata.broker.list" -> params.brokerList,
      "auto.offset.reset" -> "smallest",
      "socket.receive.buffer.size" -> "1024*1024*1024"
    )
    println(s"Create direct kafka stream, args:$kafkaParams")
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(params.topic))
  }

}
