package com.intel.PRCcloud.topologies;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.*;
import backtype.storm.tuple.*;
import storm.kafka.*;

import storm.trident.TridentState;
import storm.trident.TridentTopology;

import com.intel.PRCcloud.*;
import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.topologies.*;
import com.intel.PRCcloud.spout.*;

import java.util.HashMap;
import java.util.Map;

public class SingleTridentSpoutTops {

  protected StormBenchConfig config;
  
  public SingleTridentSpoutTops(StormBenchConfig c){
    this.config = c;
  }

  public void run() throws Exception{
    StormSubmitter.submitTopology(config.benchName, getConf(), createTridentTopology().build());
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
  
  public TridentTopology createTridentTopology(){
    TridentTopology topology = new TridentTopology();
    setTopology(topology);
    return topology;
  }

  public void setTopology(TridentTopology topology) {
    
  }
  
}
