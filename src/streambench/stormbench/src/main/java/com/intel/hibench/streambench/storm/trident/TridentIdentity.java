package com.intel.hibench.streambench.storm.trident;


import backtype.storm.tuple.*;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import storm.kafka.trident.*;

import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.spout.*;
import com.intel.hibench.streambench.storm.topologies.*;

public class TridentIdentity extends SingleTridentSpoutTops {
  
  public TridentIdentity(StormBenchConfig config){
    super(config);
  }

  @Override
  public void setTopology(TridentTopology topology) {
    OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

    topology
      .newStream("bg0", spout)
      .each(spout.getOutputFields(), new Identity(), new Fields("tuple"))
      .parallelismHint(config.workerCount);
  }
  public static class Identity extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector){
      collector.emit(new Values(tuple.getValues()));
    }
  }
}
