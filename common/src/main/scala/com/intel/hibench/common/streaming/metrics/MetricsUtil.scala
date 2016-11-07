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
package com.intel.hibench.common.streaming.metrics

import com.intel.hibench.common.streaming.Platform
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

object MetricsUtil {

  val TOPIC_CONF_FILE_NAME = "metrics_topic.conf"

  def getTopic(platform: Platform, sourceTopic: String, producerNum: Int,
               recordPerInterval: Long, intervalSpan: Int): String = {
    val topic = s"${platform}_${sourceTopic}_${producerNum}_${recordPerInterval}" +
      s"_${intervalSpan}_${System.currentTimeMillis()}"
    println(s"metrics is being written to kafka topic $topic")
    topic
  }

  def createTopic(zkConnect: String, topic: String, partitions: Int): Unit = {
    val zkClient = new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer)
    try {
      AdminUtils.createTopic(zkClient, topic, partitions, 1)
      while (!AdminUtils.topicExists(zkClient, topic)) {
        Thread.sleep(100)
      }
    } catch {
      case e: Exception =>
        throw e
    } finally {
      zkClient.close()
    }
  }
}
