package com.intel.hibench.streambench.storm.topologies;

import backtype.storm.topology.*;
import backtype.storm.*;

import com.intel.hibench.streambench.storm.util.*;

public class SingleSpoutTops extends AbstractStormSpoutTops {

    public SingleSpoutTops(StormBenchConfig c) {
        super(c);
    }

    public void run() throws Exception {
        StormSubmitter.submitTopology(config.benchName, getConf(), getBuilder().createTopology());
    }

    public Config getConf() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(200);
        conf.put("topology.spout.max.batch.size", 64 * 1024);
        conf.setNumWorkers(config.workerCount);
        if (!config.ackon)
            conf.setNumAckers(0);
        return conf;
    }

    public TopologyBuilder getBuilder() {
        TopologyBuilder builder = new TopologyBuilder();
        setSpout(builder);
        setBolt(builder);
        return builder;
    }


}
