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


public class SketchStream extends SingleSpoutTops{
	
	public SketchStream(StormBenchConfig config){
	  super(config);
	}
	
	public void setBolt(TopologyBuilder builder){
      builder.setBolt("sketchAndPrint",new SketchStreamBolt(config.fieldIndex,config.separator),config.boltThreads).shuffleGrouping("spout");
    }
	
	
}
