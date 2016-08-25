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
package com.intel.hibench.streambench.storm.trident.functions;

import com.google.common.collect.ImmutableMap;
import com.intel.hibench.streambench.common.metrics.KafkaReporter;
import com.intel.hibench.streambench.common.metrics.LatencyReporter;
import com.intel.hibench.streambench.storm.util.StormBenchConfig;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class Identity extends BaseFunction {
  private final StormBenchConfig config;
  private LatencyReporter reporter = null;

  public Identity(StormBenchConfig config) {
    this.config = config;
  }

  @Override
  public void prepare(Map conf, TridentOperationContext context) {
    this.reporter = new KafkaReporter(config.reporterTopic, config.brokerList);
  }

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    ImmutableMap<String, String> kv = (ImmutableMap<String, String>) tuple.getValue(0);
    collector.emit(new Values(kv));
    reporter.report(Long.parseLong(kv.keySet().iterator().next()),
        System.currentTimeMillis());
  }
}
