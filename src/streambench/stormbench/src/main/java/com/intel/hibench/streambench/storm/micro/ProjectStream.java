package com.intel.hibench.streambench.storm.micro;

import backtype.storm.topology.*;

import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.topologies.*;


public class ProjectStream extends SingleSpoutTops{
	
	public ProjectStream(StormBenchConfig config){
	  super(config);
	}
	
	public void setBolt(TopologyBuilder builder){
      builder.setBolt("sketchAndPrint",new ProjectStreamBolt(config.fieldIndex,config.separator),config.boltThreads).shuffleGrouping("spout");
    }
	
	
}
