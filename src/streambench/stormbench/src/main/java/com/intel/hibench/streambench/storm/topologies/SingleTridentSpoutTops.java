/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.streambench.storm.topologies;

import org.apache.storm.*;
import org.apache.storm.trident.TridentTopology;

import com.intel.hibench.streambench.storm.util.*;

public class SingleTridentSpoutTops extends AbstractTridentSpoutTops {

    protected StormBenchConfig config;

    public SingleTridentSpoutTops(StormBenchConfig c) {
        this.config = c;
    }

    public void run() throws Exception {
        StormSubmitter.submitTopology(config.benchName, getConf(), createTridentTopology().build());
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

    public TridentTopology createTridentTopology() {
        TridentTopology topology = new TridentTopology();
        setTopology(topology);
        return topology;
    }

}
