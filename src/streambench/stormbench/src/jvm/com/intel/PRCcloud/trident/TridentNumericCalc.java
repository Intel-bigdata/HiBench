package com.intel.PRCcloud.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.*;
import backtype.storm.topology.base.BaseRichSpout;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import storm.kafka.trident.*;


import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.spout.*;
import com.intel.PRCcloud.topologies.*;

public class TridentNumericCalc extends SingleTridentSpoutTops {

  public TridentNumericCalc(StormBenchConfig config){
    super(config);
  }

  @Override
  public void setTopology(TridentTopology topology) {
    OpaqueTridentKafkaSpout spout = ConstructSpoutUtil.constructTridentSpout();

    topology
      .newStream("bg0", spout)
      .each(spout.getOutputFields(), new NumericCalc(config.separator, config.fieldIndex), new Fields("max", "min", "sum", "count"))
      .parallelismHint(config.workerCount);
  }

  public static class NumericCalc extends BaseFunction {
    private int fieldIndexInner;
    private String separatorInner;
    private long max=0;
    private long min=10000;
    private long sum=0;
    private long count=0;
  
    public NumericCalc(String separator,int fieldIndex) {
          this.separatorInner = separator;
          this.fieldIndexInner = fieldIndex;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector){
      String record = tuple.getString(0);
      String[] fields = record.trim().split(separatorInner);
      if (fields.length > fieldIndexInner) {
        long val = Long.parseLong(fields[fieldIndexInner]);
        if(val>max) max = val;
        if(val<min) min = val;
        sum += val;
        count += 1;
        double avg = (double)sum/(double)count;
        collector.emit(new Values(max,min,sum,avg,count));
      }
    }
  }
}
