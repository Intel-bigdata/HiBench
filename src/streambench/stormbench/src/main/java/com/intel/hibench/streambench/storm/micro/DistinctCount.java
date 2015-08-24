package com.intel.hibench.streambench.storm.micro;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.tuple.*;

import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.topologies.*;

import java.util.HashSet;
import java.util.Set;

public class DistinctCount extends SingleSpoutTops{
  
  public DistinctCount(StormBenchConfig config){
    super(config);
  }
  
  public void setBolt(TopologyBuilder builder){
          builder.setBolt("sketch",new ProjectStreamBolt(config.fieldIndex,config.separator),config.boltThreads).shuffleGrouping("spout");
		  builder.setBolt("distinct",new TotalDistinctCountBolt(),config.boltThreads).fieldsGrouping("sketch", new Fields("field"));
  }
  
  public static class TotalDistinctCountBolt extends BaseBasicBolt {
    Set<String> set = new HashSet<String>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector){
      String word = tuple.getString(0); //FIXME: always pick up index 0? should be configurable according to sparkstream's version
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
