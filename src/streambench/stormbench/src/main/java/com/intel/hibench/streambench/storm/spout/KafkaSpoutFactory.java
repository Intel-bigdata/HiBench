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
import backtype.storm.Config;
import storm.kafka.*;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.trident.*;

public class KafkaSpoutFactory{
  
  public static BaseRichSpout getSpout(String zkHost,String topic,String consumerGroup,boolean readFromStart){
    BrokerHosts brokerHosts=new ZkHosts(zkHost);
    SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,"/"+consumerGroup,consumerGroup); 
    spoutConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
    //spoutConfig.stateUpdateIntervalMs = 1000;
	spoutConfig.forceFromStart=readFromStart;
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
	return kafkaSpout;
  }
  

  public static OpaqueTridentKafkaSpout getTridentSpout(String zkHost,String topic,String consumerGroup,boolean readFromStart){
    BrokerHosts brokerHosts=new ZkHosts(zkHost);
	TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(brokerHosts,topic,consumerGroup);
    tridentKafkaConfig.fetchSizeBytes = 10*1024;
    tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    tridentKafkaConfig.forceFromStart = readFromStart;
    OpaqueTridentKafkaSpout opaqueTridentKafkaSpout = new OpaqueTridentKafkaSpout(tridentKafkaConfig);
	return opaqueTridentKafkaSpout;
  }
  
  
}
