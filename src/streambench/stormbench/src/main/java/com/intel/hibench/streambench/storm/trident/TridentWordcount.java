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

package com.intel.hibench.streambench.storm.trident;

import com.intel.hibench.streambench.common.metrics.KafkaReporter;
import com.intel.hibench.streambench.common.metrics.LatencyReporter;
import com.intel.hibench.streambench.storm.spout.KafkaSpoutFactory;
import com.intel.hibench.streambench.storm.topologies.SingleTridentSpoutTops;
import com.intel.hibench.streambench.storm.trident.functions.Parser;
import com.intel.hibench.streambench.storm.util.StormBenchConfig;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class TridentWordcount extends SingleTridentSpoutTops {

  public TridentWordcount(StormBenchConfig config) {
    super(config);
  }

  @Override
  public TridentTopology createTopology() {
    ITridentDataSource source = KafkaSpoutFactory.getTridentSpout(config, true);

    TridentTopology topology = new TridentTopology();
    topology.newStream("kafka", source)
        .each(new Fields("str"), new Parser(), new Fields("ip", "time"))
        .project(new Fields("ip", "time"))
        .parallelismHint(config.spoutThreads)
        .groupBy(new Fields("ip"))
        .aggregate(new Fields("ip", "time"), new Count(config), new Fields("word", "count"))
        .parallelismHint(config.boltThreads);
    return topology;
  }

  private static class Count implements Aggregator<Count.State> {

    private final StormBenchConfig config;
    private LatencyReporter reporter = null;

    static class State {
      String ip;
      long count = 0;

    }

    Count(StormBenchConfig config) {
      this.config = config;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
      this.reporter = new KafkaReporter(config.reporterTopic, config.brokerList);
    }

    @Override
    public State init(Object o, TridentCollector tridentCollector) {
      return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tridentTuple, TridentCollector tridentCollector) {
      state.ip = tridentTuple.getString(0);
      state.count++;
      tridentCollector.emit(new Values(state.ip, state.count));
      reporter.report(tridentTuple.getLong(0), System.currentTimeMillis());
    }

    @Override
    public void complete(State state, TridentCollector tridentCollector) {
    }

    @Override
    public void cleanup() {
    }
  }
}
