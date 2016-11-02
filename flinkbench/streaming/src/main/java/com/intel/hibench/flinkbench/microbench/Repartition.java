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

package com.intel.hibench.flinkbench.microbench;

import com.intel.hibench.flinkbench.datasource.StreamBase;
import com.intel.hibench.flinkbench.util.FlinkBenchConfig;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.intel.hibench.common.streaming.metrics.KafkaReporter;

public class Repartition extends StreamBase {

  @Override
  public void processStream(final FlinkBenchConfig config) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setBufferTimeout(config.bufferTimeout);

    createDataStream(config);
    DataStream<Tuple2<String, String>> dataStream = env.addSource(getDataStream());

    dataStream.rebalance().map(
        new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

          @Override
          public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {
            KafkaReporter kafkaReporter = new KafkaReporter(config.reportTopic, config.brokerList);

            kafkaReporter.report(Long.parseLong(value.f0), System.currentTimeMillis());
            return value;
          }
        });


    env.execute("Repartition Job");
  }
}
