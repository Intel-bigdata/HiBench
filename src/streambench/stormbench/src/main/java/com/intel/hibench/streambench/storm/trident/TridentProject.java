package com.intel.hibench.streambench.storm.trident;

import backtype.storm.tuple.Fields;
import storm.trident.TridentTopology;
import storm.kafka.trident.*;

import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.spout.*;
import com.intel.hibench.streambench.storm.topologies.*;

public class TridentProject extends SingleTridentSpoutTops {

  public TridentProject(StormBenchConfig config) {
    super(config);
  }

  @Override
  public void setTopology(TridentTopology topology) {
    OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

    topology
      .newStream("bg0", spout)
      .each(spout.getOutputFields(), new Sketch(config.fieldIndex, config.separator), new Fields("field"))
      .parallelismHint(config.workerCount);
  }

}
