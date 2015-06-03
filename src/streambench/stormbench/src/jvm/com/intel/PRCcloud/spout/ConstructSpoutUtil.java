package com.intel.PRCcloud.spout;

import backtype.storm.topology.base.BaseRichSpout;
import com.intel.PRCcloud.util.*;
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
