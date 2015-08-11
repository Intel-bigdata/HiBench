package com.intel.PRCcloud.micro;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.intel.PRCcloud.util.*;
import backtype.storm.tuple.Fields;

public class ProjectStreamBolt extends BaseBasicBolt{
	private int fieldIndex;
	private String separator;
	
	public ProjectStreamBolt(int fieldIndex, String separator){
		this.fieldIndex=fieldIndex;
		this.separator=separator;
	}
	
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String record=tuple.getString(0);
		String[] fields=record.split(separator);
		if(fields.length>fieldIndex){
		  //BenchLogUtil.logMsg(fields[fieldIndex]);
		  collector.emit(new Values(fields[fieldIndex]));
		}
			
	  }
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  declarer.declare(new Fields("field"));
	}
}
