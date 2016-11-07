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
import com.intel.hibench.common.streaming.UserVisit;
import com.intel.hibench.common.streaming.UserVisitParser;
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

import java.util.HashMap;
import java.util.Map;

public class WordCount extends SingleSpoutTops {

  public WordCount(StormBenchConfig config) {
    super(config);
  }

  @Override
  public void setBolts(TopologyBuilder builder) {
    BoltDeclarer boltDeclarer = builder.setBolt("split", new SplitStreamBolt(),
        config.boltThreads);
    if (config.localShuffle) {
      boltDeclarer.localOrShuffleGrouping("spout");
    } else {
      boltDeclarer.shuffleGrouping("spout");
    }
    builder.setBolt("count", new WordCountBolt(config),
        config.boltThreads).fieldsGrouping("split", new Fields("ip"));
  }

  private static class WordCountBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    private final StormBenchConfig config;

    WordCountBolt(StormBenchConfig config) {
      this.config = config;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);

      LatencyReporter latencyReporter = new KafkaReporter(config.reporterTopic, config.brokerList);
      latencyReporter.report(tuple.getLong(1), System.currentTimeMillis());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("ip", "count"));
    }
  }

  private static class SplitStreamBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      ImmutableMap<String, String> kv = (ImmutableMap<String, String>) tuple.getValue(0);
      String key = kv.keySet().iterator().next();
      Long startTime = Long.parseLong(key);
      UserVisit uv = UserVisitParser.parse(kv.get(key));
      collector.emit(new Values(uv.getIp(), startTime));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("ip", "time"));
    }
  }

}
