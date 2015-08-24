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

import java.util.Set;
import java.util.HashSet;

public class TridentDistinctCount extends SingleTridentSpoutTops {

  public TridentDistinctCount(StormBenchConfig config) {
    super(config);
  }

  @Override
  public void setTopology(TridentTopology topology) {
    OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

    topology
      .newStream("bg0", spout)
      .each(spout.getOutputFields(), new Sketch(config.fieldIndex, config.separator), new Fields("field"))
      .parallelismHint(config.spoutThreads)
      .partitionBy(new Fields("field"))
      .each(new Fields("field"), new DistinctCount(), new Fields("size"))
      .parallelismHint(config.workerCount);
  }

  public static class DistinctCount extends BaseFunction {
    Set<String> set = new HashSet<String>();

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String word = tuple.getString(0);
      set.add(word);
      BenchLogUtil.logMsg("Distinct count:"+set.size());
      collector.emit(new Values(set.size()));
    }


  }
}
