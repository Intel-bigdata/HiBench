package com.intel.hibench.streambench.storm.micro;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.tuple.*;

import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.topologies.*;


public class Identity extends SingleSpoutTops{
  
  public Identity(StormBenchConfig config){
    super(config);
  }
  
  public void setBolt(TopologyBuilder builder){
	  builder.setBolt("identity",new IdentityBolt(), config.boltThreads).shuffleGrouping("spout");
  }
  
  public static class IdentityBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector){
      collector.emit(new Values(tuple.getValues()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("tuple"));
    }
  }
}

