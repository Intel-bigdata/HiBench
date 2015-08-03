package com.intel.PRCcloud;

import com.intel.PRCcloud.streamBench.util.ConfigLoader;
import com.intel.PRCcloud.util.*;
import com.intel.PRCcloud.micro.*;
import com.intel.PRCcloud.trident.*;
import com.intel.PRCcloud.metrics.Reporter;
import com.intel.PRCcloud.spout.*;

public class RunBench{
 
  public static void main(String[] args) throws Exception{
	runAll(args);
  }
  
  public static void runAll(String[] args) throws Exception{
  
//	if(args.length<12)
//	    BenchLogUtil.handleError("RunBench Usage:<NIMBUS> <NIMBUS_APIPORT> <ZKHOST> <WORKER_COUNT> <SPOUT_THREADS> <BOLT_THREADS> <BENCHNAME> <RECORD_COUNT> <TOPIC> <CONSUMER_GROUP>");
	  if (args.length<1)
		  BenchLogUtil.handleError("Usage: RunBench <ConfigFile>");

    StormBenchConfig conf=new StormBenchConfig();
//	conf.nimbus=args[0];
//	conf.nimbusAPIPort=Integer.parseInt(args[1]);
//	conf.zkHost=args[2];
//	conf.workerCount=Integer.parseInt(args[3]);
//	conf.spoutThreads=Integer.parseInt(args[4]);
//	conf.boltThreads=Integer.parseInt(args[5]);
//	conf.benchName=args[6];
//	conf.recordCount=Long.parseLong(args[7]);
//	conf.topic=args[8];
//	conf.consumerGroup=args[9];
//	conf.readFromStart=Boolean.parseBoolean(args[10]);
//	conf.ackon=Boolean.parseBoolean(args[11]);
//	conf.nimbusContactInterval=Integer.parseInt(args[12]);

	  ConfigLoader cl    = new ConfigLoader(args[0]);
	  conf.nimbus        = cl.getPropertiy("hibench.streamingbench.storm.nimbus");
	  conf.nimbusAPIPort = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.storm.nimbusAPIPort"));
	  conf.zkHost        = cl.getPropertiy("hibench.streamingbench.zookeeper.host");
	  conf.workerCount   = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.storm.worker_count"));
	  conf.spoutThreads  = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.storm.spout_threads"));
	  conf.boltThreads   = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.storm.bolt_threads"));
	  conf.benchName     = cl.getPropertiy("hibench.streamingbench.benchname");
	  conf.recordCount   = Long.parseLong(cl.getPropertiy("hibench.streamingbench.record_count"));
	  conf.topic         = cl.getPropertiy("hibench.streamingbench.topic_name");
	  conf.consumerGroup = cl.getPropertiy("hibench.streamingbench.consumer_group");
	  conf.readFromStart = Boolean.parseBoolean(cl.getPropertiy("hibench.streamingbench.storm.read_from_start"));
	  conf.ackon         = Boolean.parseBoolean(cl.getPropertiy("hibench.streamingbench.storm.ackon"));
	  conf.nimbusContactInterval = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.storm.nimbusContactInterval"));

//	boolean isLocal=false;
//	String mode=prop.getProperty("mode");
//	int nextIndex=0;
//	if(mode!=null&&mode.equals("localSpout")){
//	  isLocal=true;
//	  nextIndex=2;
//	}else nextIndex=4;
	
	boolean isLocal=false;

	ConstructSpoutUtil.setConfig(conf);
	
	String benchName=conf.benchName;	

	BenchLogUtil.logMsg("Benchmark starts... local:" + isLocal + "  " + benchName);
	
	if(benchName.equals("micro-identity")) {
		Identity identity = new Identity(conf);
		identity.run();
	}else if(benchName.equals("micro-project")) {
//	  if(args.length<nextIndex+2){
//	    BenchLogUtil.handleError("RunBench Usage: micro-project <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		conf.fieldIndex = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.field_index"));
		ProjectStream project = new ProjectStream(conf);
		project.run();
	}else if(benchName.equals("micro-sample")) {
//	  if(args.length<nextIndex+1){
//	    BenchLogUtil.handleError("RunBench Usage: micro-sample <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <PROBABILITY>");
//	  }
		conf.prob = Double.parseDouble(cl.getPropertiy("hibench.streamingbench.prob"));
		SampleStream sample = new SampleStream(conf);
		sample.run();
	}else if(benchName.equals("micro-wordcount")) {
//	  if(args.length<nextIndex+1){
//	    BenchLogUtil.handleError("RunBench Usage: micro-wordcount <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		Wordcount wordcount = new Wordcount(conf);
		wordcount.run();
	}else if(benchName.equals("micro-grep")){
//	  if(args.length<nextIndex+1){
//	    BenchLogUtil.handleError("RunBench Usage: micro-grep <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <PATTERN>");
//	  }
	  conf.pattern = cl.getPropertiy("hibench.streamingbench.pattern");
	  GrepStream grep=new GrepStream(conf);
	  grep.run();
	}else if(benchName.equals("micro-statistics")) {
//	  if(args.length<nextIndex+2){
//	    BenchLogUtil.handleError("RunBench Usage: micro-statisics <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		conf.fieldIndex = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.field_index"));
		NumericCalc numeric = new NumericCalc(conf);
		numeric.run();
	}else if(benchName.equals("micro-distinctcount")) {
//	  if(args.length<nextIndex+2){
//	    BenchLogUtil.handleError("RunBench Usage: micro-distinctcount <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		conf.fieldIndex = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.field_index"));
		DistinctCount distinct = new DistinctCount(conf);
		distinct.run();
	}else if(benchName.equals("trident-wordcount")) {
//	  if(args.length<nextIndex+1){
//	    BenchLogUtil.handleError("RunBench Usage: trident-wordcount <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		TridentWordcount wordcount = new TridentWordcount(conf);
		wordcount.run();
	}else if(benchName.equals("trident-identity")){
//	  if(args.length<nextIndex){
//	    BenchLogUtil.handleError("RunBench Usage: trident-identity <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>]");
//	  }
	  TridentIdentity identity=new TridentIdentity(conf);
	  identity.run();
	}else if(benchName.equals("trident-sample")) {
//	  if (args.length < nextIndex+1) {
//	    BenchLogUtil.handleError("RunBench Usage: trident-sample <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <PROBABILITY>");
//	  }
		conf.prob = Double.parseDouble(cl.getPropertiy("hibench.streamingbench.prob"));
		TridentSample sample = new TridentSample(conf);
		sample.run();
	}else if(benchName.equals("trident-sketch")) {
//	  if (args.length < nextIndex+2) {
//	    BenchLogUtil.handleError("RunBench Usage: trident-project <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		conf.fieldIndex = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.field_index"));
		TridentProject project = new TridentProject(conf);
		project.run();
	}else if(benchName.equals("trident-grep")){
//	  if (args.length < nextIndex+1) {
//	    BenchLogUtil.handleError("RunBench Usage: trident-grep <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <PATTERN>");
//	  }
	  conf.pattern = cl.getPropertiy("hibench.streamingbench.pattern");
	  TridentGrep grep = new TridentGrep(conf);
	  grep.run();
	}else if(benchName.equals("trident-distinctcount")) {
//	  if(args.length < nextIndex+2) {
//	    BenchLogUtil.handleError("RunBench Usage: trident-distinctcount <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		conf.fieldIndex = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.field_index"));
		TridentDistinctCount distinct = new TridentDistinctCount(conf);
		distinct.run();
	}else if(benchName.equals("micro-statisticssep")) {
//	  if(args.length<nextIndex+2){
//	    BenchLogUtil.handleError("RunBench Usage: micro-statisicssep <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		conf.fieldIndex = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.field_index"));
		NumericCalcSep numeric = new NumericCalcSep(conf);
		numeric.run();
	}else if(benchName.equals("trident-statistics")) {
//	  if (args.length < nextIndex+2) {
//	    BenchLogUtil.handleError("RunBench Usage: trident-statisics <RECORD_COUNT> [<TOPIC> <CONSUMER_GROUP>] <SEPARATOR> <FIELD_INDEX>");
//	  }
		conf.separator = cl.getPropertiy("hibench.streamingbench.separator");
		conf.fieldIndex = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.field_index"));
		TridentNumericCalc numeric = new TridentNumericCalc(conf);
		numeric.run();
	}
	
	//Collect metrics data
	Thread metricCollector=new Thread(new Reporter(conf.nimbus,conf.nimbusAPIPort,conf.benchName,conf.recordCount,conf.nimbusContactInterval));
	metricCollector.start();

  }
}
