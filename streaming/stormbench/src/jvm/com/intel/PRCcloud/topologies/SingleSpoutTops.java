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

  String benchName;
  protected int workerCount;
  int spoutThreads;
  
  public SingleSpoutTops(String benchName,int workerCount,int spoutThreads){
    this.benchName=benchName;
    this.workerCount=workerCount;
	this.spoutThreads=spoutThreads;
  }

  public void run() throws Exception{
    StormSubmitter.submitTopology(benchName, getConf(), getBuilder().createTopology());
  }
  
  public Config getConf(){
    Config conf = new Config();
    conf.setMaxTaskParallelism(200);
	conf.put("topology.spout.max.batch.size",64*1024);
	conf.setNumWorkers(workerCount);
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
	builder.setSpout("spout", spout, spoutThreads);
  }
  
  public void setBolt(TopologyBuilder builder){
    
  }
  
  
}
