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
package com.intel.hibench.gearpumpbench.source

import java.util.Properties

import akka.actor.ActorSystem
import com.intel.hibench.gearpumpbench.util.GearpumpConfig
import com.twitter.bijection.Injection
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.streaming.transaction.api.MessageDecoder

class KafkaSourceProvider(implicit actorSystem: ActorSystem) extends SourceProvider {
  override def getSourceProcessor(conf: GearpumpConfig): Processor[_ <: Task] = {
    getKafkaSource(conf.zkHost, conf.brokerList, conf.topic, conf.parallelism)
  }

  private def getKafkaSource(zkConnect: String, bootstrapServers: String, topic: String, parallelism: Int): Processor[_ <: Task] = {
    val props = new Properties
    props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect)
    props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(KafkaConfig.FETCH_SLEEP_MS_CONFIG, new Integer(1))
    props.put(KafkaConfig.MESSAGE_DECODER_CLASS_CONFIG, classOf[KeyValueDecoder])
    props.put(KafkaConfig.CONSUMER_START_OFFSET_CONFIG, new java.lang.Long(-1))

    val kafkaSource = new KafkaSource(topic, props)
    DataSourceProcessor(kafkaSource, parallelism)
  }
}

class KeyValueDecoder extends MessageDecoder {
  override def fromBytes(key: Array[Byte], value: Array[Byte]): Message = {
    Message(Injection.invert[String, Array[Byte]](value).get,
      Injection.invert[String, Array[Byte]](key).get.toLong)
  }
}
