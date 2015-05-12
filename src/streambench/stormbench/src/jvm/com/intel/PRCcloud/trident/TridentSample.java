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

public class TridentSample extends SingleTridentSpoutTops {
  private double probability;
  public TridentSample(StormBenchConfig config){
    super(config);
  }

  @Override
  public void setTopology(TridentTopology topology) {
    OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

    topology
      .newStream("bg0", spout)
      .each(spout.getOutputFields(), new Sample(config.prob), new Fields("tuple"))
      .parallelismHint(config.workerCount);
  }
  public static class Sample extends BaseFunction {
    private double probability;
    private int count = 0;

    public Sample(double prob) {
      probability = prob;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector){
      double randVal = Math.random();
      if (randVal <= probability) {
        count += 1;
        collector.emit(new Values(tuple.getString(0)));
        BenchLogUtil.logMsg("   count:" + count);
      }
    }
  }
}
