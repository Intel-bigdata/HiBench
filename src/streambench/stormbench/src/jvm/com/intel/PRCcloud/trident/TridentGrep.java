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
