package com.intel.PRCcloud.micro;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SplitStreamBolt extends BaseBasicBolt{
	private String separator;
	
	public SplitStreamBolt(String separator){
		this.separator=separator;
	}
	
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String record=tuple.getString(0);
		String[] fields=record.split(separator);
		for(String s:fields){
		  collector.emit(new Values(s));
		}
	  }
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  declarer.declare(new Fields("word"));
	}
}
