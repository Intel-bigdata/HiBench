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

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
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
