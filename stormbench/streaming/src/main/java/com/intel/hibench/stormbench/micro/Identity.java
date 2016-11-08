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

package com.intel.hibench.stormbench.micro;

import com.google.common.collect.ImmutableMap;
import com.intel.hibench.common.streaming.metrics.KafkaReporter;
import com.intel.hibench.common.streaming.metrics.LatencyReporter;
import com.intel.hibench.stormbench.topologies.SingleSpoutTops;
import com.intel.hibench.stormbench.util.StormBenchConfig;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class Identity extends SingleSpoutTops {

  public Identity(StormBenchConfig config) {
    super(config);
  }

  @Override
  public void setBolts(TopologyBuilder builder) {
    BoltDeclarer boltDeclarer = builder.setBolt("identity", new IdentityBolt(config),
        config.boltThreads);
    if (config.localShuffle) {
      boltDeclarer.localOrShuffleGrouping("spout");
    } else {
      boltDeclarer.shuffleGrouping("spout");
    }
  }

  private static class IdentityBolt extends BaseBasicBolt {

    private final StormBenchConfig config;

    IdentityBolt(StormBenchConfig config) {
      this.config = config;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      final LatencyReporter latencyReporter = new KafkaReporter(config.reporterTopic, config.brokerList);
      ImmutableMap<String, String> kv = (ImmutableMap<String, String>) tuple.getValue(0);
      collector.emit(new Values(kv));
      latencyReporter.report(Long.parseLong(kv.keySet().iterator().next()), System.currentTimeMillis());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("tuple"));
    }
  }
}
