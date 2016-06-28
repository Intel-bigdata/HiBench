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

import com.intel.hibench.streambench.storm.spout.KafkaSpoutFactory;
import com.intel.hibench.streambench.storm.topologies.SingleTridentSpoutTops;
import com.intel.hibench.streambench.storm.util.StormBenchConfig;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;




public class TridentNumericCalc extends SingleTridentSpoutTops implements Serializable {

  public TridentNumericCalc(StormBenchConfig config) {
    super(config);
  }

  @Override
  public TridentTopology createTopology() {
    OpaqueTridentKafkaSpout spout = KafkaSpoutFactory.getTridentSpout(config);

    TridentTopology topology = new TridentTopology();
    topology.newStream("bg0", spout)
            .parallelismHint(config.spoutThreads)
            .each(spout.getOutputFields(), new Trim(config.separator, config.fieldIndex),
                    new Fields("number"))
            .persistentAggregate(new MemoryMapState.Factory(),
                    new Fields("number"), new NumericCalc(), new Fields("res"));
    return topology;
  }

  private static class Trim extends BaseFunction {
    String separator;
    int fieldIndex;

    public Trim(String separator, int fieldIndex) {
      this.separator = separator;
      this.fieldIndex = fieldIndex;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String record = tuple.getString(0);
      String[] fields = record.trim().split(separator);
      if (fields.length > fieldIndex) {
        Long val = Long.parseLong(fields[fieldIndex]);
        collector.emit(new Values(val));
      }
    }
  }

  private static class Numeric implements Serializable {
    public Long max = 0L;
    public Long min = 10000L;
    public Long sum = 0L;
    public Long count = 0L;

    public Numeric() {
    }

    public Numeric(Long max, Long min, Long sum, Long count) {
      this.max = max;
      this.min = min;
      this.sum = sum;
      this.count = count;
    }
  }

  private static class NumericCalc implements CombinerAggregator<Numeric>, Serializable {

    @Override
    public Numeric init(TridentTuple tuple) {
      if (tuple.contains("number")) {
        Long val = tuple.getLong(0);
        return new Numeric(val, val, val, 1L);
      }
      return new Numeric();
    }

    @Override
    public Numeric combine(Numeric val1, Numeric val2) {
      if (val1.max < val2.max) val1.max = val2.max;
      if (val1.min > val2.min) val1.min = val2.min;
      val1.sum += val2.sum;
      val1.count += val2.count;
      System.out.println(val1.max + " " + val1.min + " " + val1.sum + " " + val1.count);
      return val1;
    }

    @Override
    public Numeric zero() {
      return new Numeric();
    }
  }
}
