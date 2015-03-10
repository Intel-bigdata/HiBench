package com.intel.PRCcloud.spout;

import backtype.storm.topology.base.BaseRichSpout;
import com.intel.PRCcloud.util.*;
import java.io.*;
import java.util.*;
//import storm.kafka.trident.*;

public class ConstructSpoutUtil{

  private static Properties prop;
  private static String[] args;
  private static boolean isLocal=false;
  private static long recordCount;
  
  public static BaseRichSpout constructSpout(){
    BaseRichSpout spout=null;
    if(isLocal){
	  if(args.length<2)
	    BenchLogUtil.handleError("RunBench Local Usage:<BENCHNAME> <RECORD_COUNT>");
	  String dataFile=prop.getProperty("datafile");
	  spout=LocalSpoutFactory.getSpout(dataFile,recordCount);
	}else{
	  if(args.length<9)
	    BenchLogUtil.handleError("RunBench Usage:<NIMBUS> <NIMBUS_APIPORT> <ZKHOST> <WORKER_COUNT> <SPOUT_THREADS> <BENCHNAME> <RECORD_COUNT> <TOPIC> <CONSUMER_GROUP>");
	  String topic=args[7];
	  String consumerGroup=args[8];
	  String zkHost=prop.getProperty("zkHost");
	  BenchLogUtil.logMsg("Topic:"+topic+" consumerGroup:"+consumerGroup+"  zkHost:"+zkHost);
	  spout=KafkaSpoutFactory.getSpout(zkHost,topic,consumerGroup);
	}
	return spout;
  }
  
/*
  public static OpaqueTridentKafkaSpout constructTridentSpout(){
    if(args.length<9)
	    BenchLogUtil.handleError("RunBench Usage:<NIMBUS> <NIMBUS_APIPORT> <ZKHOST> <WORKER_COUNT> <SPOUT_THREADS> <BENCHNAME> <RECORD_COUNT> <TOPIC> <CONSUMER_GROUP>");
	String topic=args[7];
	String consumerGroup=args[8];
	String zkHost=prop.getProperty("zkHost");
	BenchLogUtil.logMsg("Topic:"+topic+" consumerGroup:"+consumerGroup+"  zkHost:"+zkHost);
	OpaqueTridentKafkaSpout spout=KafkaSpoutFactory.getTridentSpout(zkHost,topic,consumerGroup);
	return spout;
  }
*/
  
  public static void setArgs(Properties p,String[] a,boolean i,long r){
    prop=p;
    args=a;
	isLocal=i;
	recordCount=r;
  }
  

  
  
}