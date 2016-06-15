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

import com.intel.hibench.streambench.storm.spout.ConstructSpoutUtil;
import com.intel.hibench.streambench.storm.topologies.SingleTridentSpoutTops;
import com.intel.hibench.streambench.storm.util.BenchLogUtil;
import com.intel.hibench.streambench.storm.util.StormBenchConfig;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.kafka.trident.*;


import java.util.Map;
import java.util.HashMap;

public class TridentWordcount extends SingleTridentSpoutTops {
  
  public TridentWordcount(StormBenchConfig config){
    super(config);
  }

  @Override
  public void setTopology(TridentTopology topology) {
    OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

    topology
      .newStream("bg0", spout)
      .each(spout.getOutputFields(), new Split(config.separator), new Fields("words"))
      .parallelismHint(config.spoutThreads)
      .partitionBy(new Fields("words"))
      .each(new Fields("words"), new WordCount(), new Fields("word", "count"))
      .parallelismHint(config.workerCount)
      ;
  }

  public static class Split extends BaseFunction {
    String separator;

    public Split(String separator) {
      this.separator = separator;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(separator)) {
        collector.emit(new Values(word));
      }
    }
  }

  public static class WordCount extends BaseFunction {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      BenchLogUtil.logMsg("Word:" + word + "  count:" + count);
      collector.emit(new Values(word, count));
    }
  }

}
