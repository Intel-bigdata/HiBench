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

public class Wordcount extends SingleSpoutTops{
  private String separator;
  
  public Wordcount(String a,int w,int s,String sep){
    super(a,w,s);
    separator=sep;
  }

  public void setBolt(TopologyBuilder builder){
          builder.setBolt("split",new SplitStreamBolt(separator),workerCount).shuffleGrouping("spout");
          builder.setBolt("count",new WordCountBolt(), workerCount).fieldsGrouping("split", new Fields("word"));
  }

  public static class WordCountBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector){
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      //BenchLogUtil.logMsg("Word:"+word+"  count:"+count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

}
