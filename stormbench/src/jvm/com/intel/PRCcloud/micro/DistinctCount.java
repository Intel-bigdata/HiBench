package com.intel.PRCcloud.micro;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.tuple.*;
import storm.kafka.*;

import com.intel.PRCcloud.*;
import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.topologies.*;

import java.util.HashSet;
import java.util.Set;

public class DistinctCount extends SingleSpoutTops{
  private String separator;
  private int fieldIndex;
  
  public DistinctCount(String a,int w,int s,String sep,int index){
    super(a,w,s);
    separator=sep;
	fieldIndex=index;
  }
  
  public void setBolt(TopologyBuilder builder){
          builder.setBolt("sketch",new SketchStreamBolt(fieldIndex,separator),workerCount).shuffleGrouping("spout");
		  builder.setBolt("distinct",new TotalDistinctCountBolt(),workerCount).fieldsGrouping("sketch", new Fields("field"));
  }
  
  public static class TotalDistinctCountBolt extends BaseBasicBolt {
    Set<String> set = new HashSet<String>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector){
      String word = tuple.getString(0);
      set.add(word);
      BenchLogUtil.logMsg("Distinct count:"+set.size());
      collector.emit(new Values(set.size()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("size"));
    }
  }
  
  

}
