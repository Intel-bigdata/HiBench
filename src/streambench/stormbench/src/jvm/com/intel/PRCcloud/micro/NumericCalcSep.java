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

public class NumericCalcSep extends SingleSpoutTops{
	
	public NumericCalcSep(StormBenchConfig config){
	  super(config);
	}
	
	public void setBolt(TopologyBuilder builder){
      builder.setBolt("calc",new NumericBolt(config.separator,config.fieldIndex),config.boltThreads).shuffleGrouping("spout");
    }
	
	public static class NumericBolt extends BaseBasicBolt{
	  private int fieldIndexInner;
	  private String separatorInner;
	  private long max=0;
	  private long min=10000;
	  private long sum=0;
	  private long count=0;
	  
	  public NumericBolt(String separator,int fieldIndex){
		fieldIndexInner=fieldIndex;
		separatorInner=separator;
	  }
	  
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
		String record=tuple.getString(0);
		String[] fields=record.trim().split(separatorInner);
		if(fields.length>fieldIndexInner){
		  long val=Long.parseLong(fields[fieldIndexInner]);
		  if(val>max) max=val;
		  if(val<min) min=val;
		  sum+=val;
		  count+=1;
		  double avg=(double)sum/(double)count;
		  collector.emit(new Values(max,min,sum,avg,count));
		}
	  }
	  
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("max", "min","sum","count"));
	  }
	  
	}

}
