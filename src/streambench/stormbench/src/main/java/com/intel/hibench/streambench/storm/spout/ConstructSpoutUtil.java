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

package com.intel.hibench.streambench.storm.spout;

import backtype.storm.topology.base.BaseRichSpout;
import com.intel.hibench.streambench.storm.util.*;
import java.io.*;
import java.util.*;
import storm.kafka.trident.*;

public class ConstructSpoutUtil{

  private static StormBenchConfig conf;
  private static boolean isLocal=false;
  
  public static BaseRichSpout constructSpout(){
    BaseRichSpout spout=null;
    if(isLocal){
	  //if(args.length<2)
	    //BenchLogUtil.handleError("RunBench Local Usage:<BENCHNAME> <RECORD_COUNT>");
	  //String dataFile=prop.getProperty("datafile");
	  //spout=LocalSpoutFactory.getSpout(dataFile,recordCount);
	  spout=null;
	}else{
	  String topic=conf.topic;
	  String consumerGroup=conf.consumerGroup;
	  boolean readFromStart=conf.readFromStart;
	  String zkHost=conf.zkHost;
	  BenchLogUtil.logMsg("Topic:"+topic+" consumerGroup:"+consumerGroup+"  zkHost:"+zkHost);
	  spout=KafkaSpoutFactory.getSpout(zkHost,topic,consumerGroup,readFromStart);
	}
	return spout;
  }
  

  public static OpaqueTridentKafkaSpout constructTridentSpout(){
	String topic = conf.topic;
	String consumerGroup = conf.consumerGroup;
	boolean readFromStart=conf.readFromStart;
	String zkHost = conf.zkHost;
	BenchLogUtil.logMsg("Topic:"+topic+" consumerGroup:"+consumerGroup+"  zkHost:"+zkHost);
	OpaqueTridentKafkaSpout spout=KafkaSpoutFactory.getTridentSpout(zkHost,topic,consumerGroup,readFromStart);
	return spout;
  }

  
  public static void setConfig(StormBenchConfig c){
    conf=c;
  }
  

  
  
}
