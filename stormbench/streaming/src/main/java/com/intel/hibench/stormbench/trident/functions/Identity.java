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
package com.intel.hibench.stormbench.trident.functions;

import com.google.common.collect.ImmutableMap;
import com.intel.hibench.common.streaming.metrics.KafkaReporter;
import com.intel.hibench.common.streaming.metrics.LatencyReporter;
import com.intel.hibench.stormbench.util.StormBenchConfig;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class Identity implements MapFunction {

  private final StormBenchConfig config;

  public Identity(StormBenchConfig config) {
    this.config = config;
  }

  @Override
  public Values execute(TridentTuple tridentTuple) {
    ImmutableMap<String, String> kv = (ImmutableMap<String, String>) tridentTuple.getValue(0);
    LatencyReporter reporter = new KafkaReporter(config.reporterTopic, config.brokerList);
    reporter.report(Long.parseLong(kv.keySet().iterator().next()),
        System.currentTimeMillis());
    return new Values(kv);
  }
}
