package com.intel.PRCcloud.micro;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.tuple.*;
import storm.kafka.*;

import com.intel.PRCcloud.*;
import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.topologies.*;

import java.util.HashMap;
import java.util.Map;

public class Identity extends SingleSpoutTops{
  
  public Identity(String a,int w,int s){
    super(a,w,s);
  }
  
  public void setBolt(TopologyBuilder builder){
	  builder.setBolt("identity",new IdentityBolt(), workerCount).shuffleGrouping("spout");
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

