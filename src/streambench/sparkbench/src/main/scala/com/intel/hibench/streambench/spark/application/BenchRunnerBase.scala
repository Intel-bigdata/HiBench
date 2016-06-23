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

package com.intel.hibench.streambench.spark.application

import com.intel.hibench.streambench.common.Logger
import com.intel.hibench.streambench.spark.util.SparkBenchConfig
import com.intel.hibench.streambench.spark.metrics.LatencyListener

import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * The base class of all test cases in spark. The sub class need to implement "process" method
  */
abstract class BenchRunnerBase(config: SparkBenchConfig, logger: Logger) {

  def process(ssc: StreamingContext, lines: DStream[(Long, String)])

  def run() = {

    // defind streaming context
    val conf = new SparkConf().setMaster(config.master).setAppName(config.appName)
    val ssc = new StreamingContext(conf, Seconds(config.batchInterval))
    ssc.checkpoint(config.checkpointPath)

    // add listener to collect static information.
    val listener = new LatencyListener(ssc, config, logger)
    ssc.addStreamingListener(listener)

    val lines: DStream[(String, String)] = if (config.directMode) {
      // direct mode with low level Kafka API
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, config.kafkaParams, Set(config.topic))

    } else {
      // receiver mode with high level Kafka API
      val kafkaInputs = (1 to config.receiverNumber).map{ _ =>
        KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
          ssc, config.kafkaParams,
          Map(config.topic -> config.threadsPerReceiver),
          config.storageLevel)
      }

      ssc.union(kafkaInputs)
    }

    // convent key from String to Long, this field stands for event creation time.
    val parsedLines = lines.map{ case (k, v) => (k.toLong, v) }
    process(ssc, parsedLines)

    ssc.start()
    ssc.awaitTermination()
  }
}
