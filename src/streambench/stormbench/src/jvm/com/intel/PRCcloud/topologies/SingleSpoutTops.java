package com.intel.PRCcloud.topologies;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.*;
import backtype.storm.tuple.*;
import storm.kafka.*;

import com.intel.PRCcloud.*;
import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.topologies.*;
import com.intel.PRCcloud.spout.*;

import java.util.HashMap;
import java.util.Map;

public class SingleSpoutTops{

  protected StormBenchConfig config;
  
  public SingleSpoutTops(StormBenchConfig c){
    config=c;
  }

  public void run() throws Exception{
    StormSubmitter.submitTopology(config.benchName, getConf(), getBuilder().createTopology());
  }
  
  public Config getConf(){
    Config conf = new Config();
    conf.setMaxTaskParallelism(200);
	conf.put("topology.spout.max.batch.size",64*1024);
	conf.setNumWorkers(config.workerCount);
	if(!config.ackon)
	  conf.setNumAckers(0);
	return conf;
  }
  
  public TopologyBuilder getBuilder(){
    TopologyBuilder builder = new TopologyBuilder();
	setSpout(builder);
	setBolt(builder);
	return builder;
  }
  
  public void setSpout(TopologyBuilder builder){
    BaseRichSpout spout=ConstructSpoutUtil.constructSpout();
	builder.setSpout("spout", spout, config.spoutThreads);
  }
  
  public void setBolt(TopologyBuilder builder){
    
  }
  
  
}
