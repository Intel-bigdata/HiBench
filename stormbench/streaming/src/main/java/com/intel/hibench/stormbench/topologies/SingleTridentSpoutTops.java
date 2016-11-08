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

package com.intel.hibench.stormbench.topologies;

import com.intel.hibench.stormbench.util.StormBenchConfig;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentTopology;

public abstract class SingleTridentSpoutTops {

  protected StormBenchConfig config;

  public SingleTridentSpoutTops(StormBenchConfig config) {
    this.config = config;
  }

  public void run() throws Exception {
    StormSubmitter.submitTopology(config.benchName, getConf(), createTopology().build());
  }

  private Config getConf() {
    Config conf = new Config();
    conf.setNumWorkers(config.workerCount);
    return conf;
  }

  public abstract TridentTopology createTopology();
}
