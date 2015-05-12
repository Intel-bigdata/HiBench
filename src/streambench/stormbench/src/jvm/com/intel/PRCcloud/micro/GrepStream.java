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

public class GrepStream extends SingleSpoutTops{
  
  public GrepStream(StormBenchConfig config) {
	  super(config);
  }
  
  public void setBolt(TopologyBuilder builder){
      builder.setBolt("grepAndPrint",new GrepBolt(config.pattern),config.boltThreads).shuffleGrouping("spout");
  }
  
  public static class GrepBolt extends BaseBasicBolt{
	  private String pattern;
	  
	  public GrepBolt(String p){
	    pattern=p;
	  }
	  
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
		String val=tuple.getString(0);
		if(val.contains(pattern)){
		  collector.emit(new Values(val));
		  //BenchLogUtil.logMsg("Matched:"+val);
		}
	  }
	  
	  public void declareOutputFields(OutputFieldsDeclarer ofd) {
	  }
	}

}
