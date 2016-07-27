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
package com.intel.hibench.streambench.common.metrics

import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer

class KafkaReporter(topic: String, bootstrapServers: String) extends LatencyReporter {

  private val props = new Properties()
  props.put("bootstrap.servers", bootstrapServers)
  private lazy val producer = ProducerSingleton.getInstance(props)

  override def report(startTime: Long, endTime: Long): Unit = {
    producer.send(new ProducerRecord[String, String](topic, 0, null, s"$startTime:$endTime"))
  }

}


object ProducerSingleton {
  private var instance : Option[KafkaProducer[String, String]] = None;

  def getInstance (props: Properties) : KafkaProducer[String, String] = synchronized {
    if (instance.isDefined) {
      instance.get
    } else {
      instance = Some(new KafkaProducer(props, new StringSerializer, new StringSerializer))
      instance.get
    }
  }
}