package com.intel.PRCcloud.micro;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.tuple.*;
import storm.kafka.*;

import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.topologies.*;


public class ProjectStream extends SingleSpoutTops{
	
	public ProjectStream(StormBenchConfig config){
	  super(config);
	}
	
	public void setBolt(TopologyBuilder builder){
      builder.setBolt("sketchAndPrint",new ProjectStreamBolt(config.fieldIndex,config.separator),config.boltThreads).shuffleGrouping("spout");
    }
	
	
}
