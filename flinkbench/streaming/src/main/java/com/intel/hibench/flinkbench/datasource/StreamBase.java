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

package com.intel.hibench.flinkbench.datasource;

import com.intel.hibench.flinkbench.util.KeyedTupleSchema;
import com.intel.hibench.flinkbench.util.FlinkBenchConfig;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;

import java.util.Properties;

public abstract class StreamBase {

  private SourceFunction<Tuple2<String, String>> dataStream;

  public SourceFunction<Tuple2<String, String>> getDataStream() {
    return this.dataStream;
  }

  public void createDataStream(FlinkBenchConfig config) throws Exception {

    Properties properties = new Properties();
    properties.setProperty("zookeeper.connect", config.zkHost);
    properties.setProperty("group.id", config.consumerGroup);
    properties.setProperty("bootstrap.servers", config.brokerList);
    properties.setProperty("auto.offset.reset", config.offsetReset);

    this.dataStream = new FlinkKafkaConsumer08<Tuple2<String, String>>(
        config.topic,
        new KeyedTupleSchema(),
        properties);
  }

  public void processStream(FlinkBenchConfig config) throws Exception {
  }
}
