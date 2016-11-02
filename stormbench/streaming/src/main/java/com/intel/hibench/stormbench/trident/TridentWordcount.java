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

package com.intel.hibench.stormbench.trident;

import com.intel.hibench.common.streaming.metrics.KafkaReporter;
import com.intel.hibench.common.streaming.metrics.LatencyReporter;
import com.intel.hibench.stormbench.spout.KafkaSpoutFactory;
import com.intel.hibench.stormbench.topologies.SingleTridentSpoutTops;
import com.intel.hibench.stormbench.trident.functions.Parser;
import com.intel.hibench.stormbench.util.StormBenchConfig;

import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

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
        .persistentAggregate(new MemoryMapState.Factory(), new Fields("ip", "time"), new Count(config),
            new Fields("word", "count"))
        .parallelismHint(config.boltThreads);
    return topology;
  }

  private static class Count implements ReducerAggregator<Count.State> {

    private final StormBenchConfig config;
    private LatencyReporter reporter = null;

    Count(StormBenchConfig config) {
      this.config = config;
    }

    @Override
    public State init() {
      this.reporter = new KafkaReporter(config.reporterTopic, config.brokerList);
      return new State();
    }

    @Override
    public State reduce(State state, TridentTuple tridentTuple) {
      state.ip = tridentTuple.getString(0);
      state.count++;
      reporter.report(tridentTuple.getLong(1), System.currentTimeMillis());
      return state;
    }

    static class State {
      String ip;
      long count = 0;
    }
  }
}
