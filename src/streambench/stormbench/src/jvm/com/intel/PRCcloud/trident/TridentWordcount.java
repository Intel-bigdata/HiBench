package com.intel.PRCcloud.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.*;
import backtype.storm.topology.base.BaseRichSpout;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import storm.kafka.trident.*;

import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.spout.*;
import com.intel.PRCcloud.topologies.*;

public class TridentWordcount extends SingleTridentSpoutTops {
  
  public TridentWordcount(StormBenchConfig config){
    super(config);
  }

  @Override
  public void setTopology(TridentTopology topology) {
    OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

    TridentState wordCounts = topology
      .newStream("bg0", spout)
      .each(spout.getOutputFields(), new Split(config.separator), new Fields("word"))
      .groupBy(new Fields("word"))
      .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
      .parallelismHint(config.workerCount);
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
}
