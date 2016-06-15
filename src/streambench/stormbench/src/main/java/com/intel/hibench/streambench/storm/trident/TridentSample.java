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

package com.intel.hibench.streambench.storm.trident;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.kafka.trident.*;


import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.spout.*;
import com.intel.hibench.streambench.storm.topologies.*;

import java.util.Random;

public class TridentSample extends SingleTridentSpoutTops {
    private double probability;
    public TridentSample(StormBenchConfig config){
        super(config);
    }

    @Override
    public void setTopology(TridentTopology topology) {
        OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

        topology
                .newStream("bg0", spout)
                .each(spout.getOutputFields(), new Sample(config.prob), new Fields("tuple"))
                .parallelismHint(config.workerCount);
    }
    public static class Sample extends BaseFunction {
        private double probability;
        private int count = 0;
        private ThreadLocal<Random> rand = null;

        public Sample(double prob) {
            probability = prob;
            rand = threadRandom(1);
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector){
            double randVal = rand.get().nextDouble();
            if (randVal <= probability) {
                count += 1;
                collector.emit(new Values(tuple.getString(0)));
                BenchLogUtil.logMsg("   count:" + count);
            }
        }

        public static ThreadLocal<Random> threadRandom(final long seed) {
            return new ThreadLocal<Random>(){
                @Override
                protected Random initialValue() {
                    return new Random(seed);
                }
            };
        }

    }
}
