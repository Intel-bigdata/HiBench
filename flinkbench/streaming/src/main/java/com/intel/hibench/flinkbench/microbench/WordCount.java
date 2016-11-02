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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.intel.hibench.common.streaming.metrics.KafkaReporter;
import com.intel.hibench.common.streaming.UserVisitParser;

public class WordCount extends StreamBase {

  @Override
  public void processStream(final FlinkBenchConfig config) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setBufferTimeout(config.bufferTimeout);
    env.enableCheckpointing(config.checkpointDuration);
    createDataStream(config);
    DataStream<Tuple2<String, String>> dataStream = env.addSource(getDataStream());
    dataStream
        .map(new MapFunction<Tuple2<String, String>, Tuple2<String, Tuple2<String, Integer>>>() {
          @Override
          public Tuple2<String, Tuple2<String, Integer>> map(Tuple2<String, String> input) throws Exception {
            String ip = UserVisitParser.parse(input.f1).getIp();
            //map record to <browser, <timeStamp, 1>> type
            return new Tuple2<String, Tuple2<String, Integer>>(ip, new Tuple2<String, Integer>(input.f0, 1));
          }
        })
        .keyBy(0)
        .map(new RichMapFunction<Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Tuple2<String, Integer>>>() {
          private transient ValueState<Integer> sum;

          @Override
          public Tuple2<String, Tuple2<String, Integer>> map(Tuple2<String, Tuple2<String, Integer>> input) throws Exception {
            int currentSum = sum.value();
            currentSum += input.f1.f1;
            sum.update(currentSum);
            KafkaReporter kafkaReporter = new KafkaReporter(config.reportTopic, config.brokerList);
            kafkaReporter.report(Long.parseLong(input.f1.f0), System.currentTimeMillis());
            return new Tuple2<String, Tuple2<String, Integer>>(input.f0, new Tuple2<String, Integer>(input.f1.f0, currentSum));
          }

          @Override
          public void open(Configuration config) {
            ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<Integer>(
                    "count", // the state name
                    TypeInformation.of(new TypeHint<Integer>() {
                    }), // type information
                    0); // default value of the state, if nothing was set
            sum = getRuntimeContext().getState(descriptor);
          }
        });
    env.execute("Word Count Job");
  }
}
