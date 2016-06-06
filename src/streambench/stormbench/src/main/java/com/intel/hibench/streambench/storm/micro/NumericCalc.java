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

import org.apache.storm.topology.base.*;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.*;

import com.intel.hibench.streambench.storm.util.*;
import com.intel.hibench.streambench.storm.topologies.*;

public class NumericCalc extends SingleSpoutTops{
	
	public NumericCalc(StormBenchConfig config){
	  super(config);
	}
	
	public void setBolt(TopologyBuilder builder){
      builder.setBolt("precalc",new NumericBolt(config.separator,config.fieldIndex),Math.max(1,config.boltThreads-1)).shuffleGrouping("spout");
	  builder.setBolt("calc",new NumericTogetherBolt(),1).globalGrouping("precalc");
    }
	
	public static class NumericBolt extends BaseBasicBolt{
	  private int fieldIndexInner;
	  private String separatorInner;
	  private long max=0;
	  private long min=Long.MAX_VALUE;
	  
	  public NumericBolt(String separator,int fieldIndex){
		fieldIndexInner=fieldIndex;
		separatorInner=separator;
	  }
	  
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
		String record=tuple.getString(0);
		String[] fields=record.trim().split(separatorInner);
		if(fields.length>fieldIndexInner){
		  long val=Long.parseLong(fields[fieldIndexInner]);
		  if(val>max) max=val;
		  if(val<min) min=val;
		  collector.emit(new Values(max,min,val,1L));
		}
	  }
	  
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("max", "min","sum","count"));
	  }
	  
	}
	
	public static class NumericTogetherBolt extends BaseBasicBolt{
	  private long max=0;
	  private long min=10000;
	  private long sum=0;
	  private long count=0;
	  
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
	    long curMax=tuple.getLong(0);
		long curMin=tuple.getLong(1);
		long curSum=tuple.getLong(2);
		long curCount=tuple.getLong(3);

		
		if(curMax>max) max=curMax;
		if(curMin<min) min=curMin;
		sum+=curSum;
		count+=curCount;
		double avg=(double)sum/(double)count;
		
		BenchLogUtil.logMsg("Max:"+max+"  Min:"+min+"  Sum:"+sum+"  Count:"+count+"  Avg:"+avg);
		
	  }
	  
	  public void declareOutputFields(OutputFieldsDeclarer ofd) {
	  }
	}

}
