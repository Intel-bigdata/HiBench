package com.intel.hibench.streambench.storm.topologies;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import com.intel.hibench.streambench.storm.spout.ConstructSpoutUtil;
import com.intel.hibench.streambench.storm.util.StormBenchConfig;

public class AbstractStormSpoutTops {
    protected StormBenchConfig config;

    public AbstractStormSpoutTops(StormBenchConfig c) {
        config=c;
    }

    public void setSpout(TopologyBuilder builder){
      BaseRichSpout spout= ConstructSpoutUtil.constructSpout();
      builder.setSpout("spout", spout, config.spoutThreads);
    }

    public void setBolt(TopologyBuilder builder){

    }
}
