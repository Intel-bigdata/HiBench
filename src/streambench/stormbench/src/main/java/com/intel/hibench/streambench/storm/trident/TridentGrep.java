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

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.kafka.trident.*;

import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.spout.*;
import com.intel.hibench.streambench.storm.topologies.*;

public class TridentGrep extends SingleTridentSpoutTops {
  
  public TridentGrep(StormBenchConfig config){
    super(config);
  }

  @Override
  public void setTopology(TridentTopology topology) {
    OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

    topology
      .newStream("bg0", spout)
      .each(spout.getOutputFields(), new Grep(config.pattern), new Fields("tuple"))
      .parallelismHint(config.workerCount);
  }

  public static class Grep extends BaseFunction {
    private String pattern;

    public Grep(String pattern) {
      this.pattern = pattern;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector){
      String val = tuple.getString(0);
      if (val.contains(pattern))
        // BenchLogUtil.logMsg(val);
        collector.emit(new Values(val));
    }
  }
}
