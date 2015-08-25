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

package com.intel.hibench.streambench.storm.micro;

import backtype.storm.topology.base.*;
import backtype.storm.topology.*;
import backtype.storm.tuple.*;

import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.topologies.*;

import java.util.HashMap;
import java.util.Map;

public class Wordcount extends SingleSpoutTops{

    public Wordcount(StormBenchConfig config){
        super(config);
    }

    public void setBolt(TopologyBuilder builder){
        builder.setBolt("split",new SplitStreamBolt(config.separator),config.boltThreads).shuffleGrouping("spout");
        builder.setBolt("count",new WordCountBolt(), config.boltThreads).fieldsGrouping("split", new Fields("word"));
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
