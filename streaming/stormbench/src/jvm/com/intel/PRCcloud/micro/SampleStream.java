package com.intel.PRCcloud.micro;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.tuple.*;
import storm.kafka.*;

import com.intel.PRCcloud.*;
import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.topologies.*;

public class SampleStream extends SingleSpoutTops{
	private double probability;
	
	public SampleStream(String a,int w,int s,double p){
	  super(a,w,s);
	  probability=p;
	}
	
	public void setBolt(TopologyBuilder builder){
      builder.setBolt("sampleAndPrint",new SampleBolt(probability),workerCount).shuffleGrouping("spout");
    }
	
	public static class SampleBolt extends BaseBasicBolt{
	  private double probability;
	  private int count=0;
	  
	  public SampleBolt(double prob){
	    probability=prob;
	  }
	  
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
	    double randVal=Math.random();
		if(randVal<=probability){
		  count+=1;
		  collector.emit(new Values(tuple.getString(0)));
		  BenchLogUtil.logMsg("   count:"+count);
		}		  
	  }
	  
	  public void declareOutputFields(OutputFieldsDeclarer ofd){
	  }
	}
}
