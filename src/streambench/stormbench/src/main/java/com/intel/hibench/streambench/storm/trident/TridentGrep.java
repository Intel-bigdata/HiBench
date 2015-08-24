package com.intel.hibench.streambench.storm.trident;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;

import storm.trident.tuple.TridentTuple;
import storm.kafka.trident.*;

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
