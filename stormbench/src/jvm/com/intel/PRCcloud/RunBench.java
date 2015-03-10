package com.intel.PRCcloud;

import java.util.*;
import java.io.*;
import java.net.URL;

import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.micro.*;
import com.intel.PRCcloud.metrics.Reporter;
import com.intel.PRCcloud.spout.*;

public class RunBench{
 
  public static void main(String[] args) throws Exception{
	runAll(args);
  }
  
  public static void runAll(String[] args) throws Exception{
    Properties prop=new Properties();
	prop.put("nimbus",args[0]);
	prop.put("nimbusAPIPort",args[1]);
	prop.put("zkHost",args[2]);
	prop.put("workerCount",args[3]);
	prop.put("spoutThreads",args[4]);
//	boolean isLocal=false;
//	String mode=prop.getProperty("mode");
//	int nextIndex=0;
//	if(mode!=null&&mode.equals("localSpout")){
//	  isLocal=true;
//	  nextIndex=2;
//	}else nextIndex=4;
	
	boolean isLocal=false;
	
	long recordCount=Long.parseLong(args[6]);
	ConstructSpoutUtil.setArgs(prop,args,isLocal,recordCount);
	
	String benchName=args[5];	
	int workerCount=Integer.parseInt(prop.getProperty("workerCount"));
	int spoutThreads=Integer.parseInt(prop.getProperty("spoutThreads"));
	
	int nextIndex=9;
	BenchLogUtil.logMsg("Benchmark starts... local:"+isLocal+"  "+benchName);
	
	if(benchName.equals("micro-identity")){
	  if(args.length<nextIndex){
	    BenchLogUtil.handleError("RunBench Usage: micro-sketch <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>]");		
	  }
	  Identity identity=new Identity(benchName,workerCount,spoutThreads);
	  identity.run();
	}else if(benchName.equals("micro-sketch")){
	  if(args.length<nextIndex+2){
	    BenchLogUtil.handleError("RunBench Usage: micro-sketch <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
	  }
	  String separator=args[nextIndex];
	  int fieldIndex=Integer.parseInt(args[nextIndex+1]);
	  SketchStream sketch=new SketchStream(benchName,workerCount,spoutThreads,separator,fieldIndex);
	  sketch.run();
	}else if(benchName.equals("micro-sample")){
	  if(args.length<nextIndex+1){
	    BenchLogUtil.handleError("RunBench Usage: micro-sample <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <PROBABILITY>");
	  }
	  double prob=Double.parseDouble(args[nextIndex]);
	  SampleStream sample=new SampleStream(benchName,workerCount,spoutThreads,prob);
	  sample.run();
	}else if(benchName.equals("micro-wordcount")){
	  if(args.length<nextIndex+1){
	    BenchLogUtil.handleError("RunBench Usage: micro-wordcount <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR>");
	  }
	  String separator=args[nextIndex];
	  Wordcount wordcount=new Wordcount(benchName,workerCount,spoutThreads,separator);
	  wordcount.run();
	}else if(benchName.equals("micro-grep")){
	  if(args.length<nextIndex+1){
	    BenchLogUtil.handleError("RunBench Usage: micro-grep <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <PATTERN>");
	  }
	  String pattern=args[nextIndex];
	  GrepStream grep=new GrepStream(benchName,workerCount,spoutThreads,pattern);
	  grep.run();
	}else if(benchName.equals("micro-statistics")){
	  if(args.length<nextIndex+2){
	    BenchLogUtil.handleError("RunBench Usage: micro-statisics <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
	  }
	  String separator=args[nextIndex];
	  int fieldIndex=Integer.parseInt(args[nextIndex+1]);
	  NumericCalc numeric=new NumericCalc(benchName,workerCount,spoutThreads,separator,fieldIndex);
	  numeric.run();
	}else if(benchName.equals("micro-distinctcount")){
	  if(args.length<nextIndex+2){
	    BenchLogUtil.handleError("RunBench Usage: micro-statisics <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
	  }
	  String separator=args[nextIndex];
	  int fieldIndex=Integer.parseInt(args[nextIndex+1]);
	  DistinctCount distinct=new DistinctCount(benchName,workerCount,spoutThreads,separator,fieldIndex);
	  distinct.run();
	}else if(benchName.equals("trident-wordcount")){
	  String separator=args[nextIndex];
	  //TridentWordcount wordcount=new TridentWordcount(benchName,workerCount,spoutThreads,separator);
	  //wordcount.run();
	}else if(benchName.equals("micro-statisticssep")){
	  if(args.length<nextIndex+2){
	    BenchLogUtil.handleError("RunBench Usage: micro-statisics <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
	  }
	  String separator=args[nextIndex];
	  int fieldIndex=Integer.parseInt(args[nextIndex+1]);
	  NumericCalcSep numeric=new NumericCalcSep(benchName,workerCount,spoutThreads,separator,fieldIndex);
	  numeric.run();
	}
	
	//Collect metrics data
	String nimbus=prop.getProperty("nimbus");
	int port=Integer.parseInt(prop.getProperty("nimbusAPIPort"));
	Thread metricCollector=new Thread(new Reporter(nimbus,port,benchName,recordCount));
	metricCollector.start();
  }
}
