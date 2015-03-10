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

	private String separator;
	private int fieldIndex;
	
	public SketchStream(String benchName,int workerCount,int spoutThreads,String separator,int fieldIndex){
	  super(benchName,workerCount,spoutThreads);
	  this.separator=separator;
	  this.fieldIndex=fieldIndex;
	}
	
	public void setBolt(TopologyBuilder builder){
      builder.setBolt("sketchAndPrint",new SketchStreamBolt(fieldIndex,separator),workerCount).shuffleGrouping("spout");
    }
	
	
}
