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
package com.intel.hibench.streambench.gearpump.source

import akka.actor.ActorSystem
import com.intel.hibench.streambench.gearpump.util.GearpumpConfig
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.streaming.kafka.lib.StringMessageDecoder
import org.apache.gearpump.streaming.kafka.{KafkaSource, KafkaStorageFactory}
import org.apache.gearpump.streaming.source.{DefaultTimeStampFilter, DataSourceProcessor}
import org.apache.gearpump.streaming.task.Task

class KafkaSourceProvider(implicit actorSystem: ActorSystem) extends SourceProvider{
  override def getSourceProcessor(conf: GearpumpConfig): Processor[_ <: Task] = {
    getKafkaSource(conf.zkHost, conf.brokerList, conf.topic, conf.partitions)
  }

  private def getKafkaSource(zkConnect: String, bootstrapServers: String, topic: String, parallelism: Int): Processor[_ <: Task] = {
    val offsetStorageFactory = new KafkaStorageFactory(zkConnect, bootstrapServers)
    val kafkaSource = new KafkaSource(topic, zkConnect,
      offsetStorageFactory, new StringMessageDecoder, new DefaultTimeStampFilter)
    DataSourceProcessor(kafkaSource, parallelism)
  }
}
