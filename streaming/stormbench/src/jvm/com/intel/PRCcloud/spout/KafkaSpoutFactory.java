package com.intel.PRCcloud.spout;

import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.Config;
import storm.kafka.*;
import backtype.storm.spout.SchemeAsMultiScheme;
//import storm.kafka.trident.*;

public class KafkaSpoutFactory{
  
  public static BaseRichSpout getSpout(String zkHost,String topic,String consumerGroup){
    BrokerHosts brokerHosts=new ZkHosts(zkHost);
    SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,"/"+consumerGroup,consumerGroup); 
    spoutConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
    //spoutConfig.stateUpdateIntervalMs = 1000;
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
	return kafkaSpout;
  }
  
/*
  public static OpaqueTridentKafkaSpout getTridentSpout(String zkHost,String topic,String consumerGroup){
    BrokerHosts brokerHosts=new ZkHosts(zkHost);
	TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(brokerHosts,topic,consumerGroup);
    tridentKafkaConfig.fetchSizeBytes = 10*1024;
    tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    OpaqueTridentKafkaSpout opaqueTridentKafkaSpout = new OpaqueTridentKafkaSpout(tridentKafkaConfig);
	return opaqueTridentKafkaSpout;
  }
*/  
  
}