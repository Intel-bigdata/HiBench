package com.intel.hibench.streambench.storm.trident;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;

import storm.trident.tuple.TridentTuple;
import storm.kafka.trident.*;


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
