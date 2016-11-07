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
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WindowedCount extends SingleSpoutTops {

  public WindowedCount(StormBenchConfig config) {
    super(config);
  }

  @Override
  public void setBolts(TopologyBuilder builder) {
    Duration windowDuration = new Duration((int) config.windowDuration, TimeUnit.MILLISECONDS);
    Duration windowSlide = new Duration((int) config.windowSlideStep, TimeUnit.MILLISECONDS);
    BoltDeclarer boltDeclarer = builder.setBolt("parser", new ParserBolt(), config.boltThreads);
    if (config.localShuffle) {
      boltDeclarer.localOrShuffleGrouping("spout");
    } else {
      boltDeclarer.shuffleGrouping("spout");
    }
    builder.setBolt("window", new SlidingWindowBolt(config)
            .withWindow(windowDuration, windowSlide),
        config.boltThreads).fieldsGrouping("parser", new Fields("ip"));
  }

  private static class ParserBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
      ImmutableMap<String, String> kv = (ImmutableMap<String, String>) tuple.getValue(0);
      String time = kv.keySet().iterator().next();
      UserVisit uv = UserVisitParser.parse(kv.get(time));
      basicOutputCollector.emit(new Values(uv.getIp(), Long.parseLong(time)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("ip", "time"));
    }
  }

  private static class SlidingWindowBolt extends BaseWindowedBolt {
    private final StormBenchConfig config;

    SlidingWindowBolt(StormBenchConfig config) {
      this.config = config;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
      Map<String, Long[]> counts = new HashMap<String, Long[]>();
      for (Tuple tuple : inputWindow.get()) {
        Long time = tuple.getLong(1);
        String ip = tuple.getString(0);
        Long[] timeAndCount = counts.get(ip);
        if (null == timeAndCount) {
          timeAndCount = new Long[2];
          timeAndCount[0] = time;
          timeAndCount[1] = 0L;
        }
        timeAndCount[0] = Math.min(timeAndCount[0], time);
        timeAndCount[1]++;
        counts.put(ip, timeAndCount);
      }
      LatencyReporter latencyReporter = new KafkaReporter(config.reporterTopic, config.brokerList);
      for (Long[] timeAndCount : counts.values()) {
        for (int i = 0; i < timeAndCount[1]; i++) {
          latencyReporter.report(timeAndCount[0], System.currentTimeMillis());
        }
      }
    }

  }
}
