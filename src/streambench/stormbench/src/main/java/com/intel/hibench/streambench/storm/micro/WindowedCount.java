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

package com.intel.hibench.streambench.storm.micro;

import com.google.common.collect.ImmutableMap;
import com.intel.hibench.streambench.common.UserVisit;
import com.intel.hibench.streambench.common.UserVisitParser;
import com.intel.hibench.streambench.common.metrics.KafkaReporter;
import com.intel.hibench.streambench.common.metrics.LatencyReporter;
import com.intel.hibench.streambench.storm.topologies.SingleSpoutTops;
import com.intel.hibench.streambench.storm.util.StormBenchConfig;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WindowedCount extends SingleSpoutTops{

  public WindowedCount(StormBenchConfig config) {
    super(config);
  }

  @Override
  public void setBolts(TopologyBuilder builder) {
    Duration windowDuration = new Duration((int)config.windowDuration, TimeUnit.MILLISECONDS);
    Duration windowSlide = new Duration((int)config.windowSlideStep, TimeUnit.MICROSECONDS);
    builder.setBolt("window", new SlidingWindowBolt(config).withWindow(windowDuration, windowSlide),
      config.boltThreads).shuffleGrouping("spout");
  }

  private static class SlidingWindowBolt extends BaseWindowedBolt {
    private final StormBenchConfig config;

    public SlidingWindowBolt(StormBenchConfig config) {
      this.config = config;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
      Map<String, Integer> counts = new HashMap<String, Integer>();
      LatencyReporter latencyReporter = new KafkaReporter(config.reporterTopic, config.brokerList);
      for(Tuple tuple: inputWindow.get()) {
        ImmutableMap<String, String> kv = (ImmutableMap<String, String>) tuple.getValue(0);
        String key = kv.keySet().iterator().next();
        Long startTime = Long.parseLong(key);
        UserVisit uv = UserVisitParser.parse(kv.get(key));
        latencyReporter.report(startTime, System.currentTimeMillis());

        Integer count = counts.get(uv.getIp());
        if (count == null) {
          count = 0;
        }
        count++;
        counts.put(uv.getIp(), count);
      }
    }
  }
}
