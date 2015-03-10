package com.intel.PRCcloud.streamBench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import com.intel.PRCcloud.streamBench.util._
import com.intel.PRCcloud.streamBench.microbench._

object RunBench{
	def main(args:Array[String]){
		//val args2=Array("micro/identity","spark_test_1_1","spark://sr119:7077","1","sr464:2181","spark_consumer_1","1","100000")
		this.run(args)
	}

	def run(args:Array[String]){
		if(args.length < 8){
			BenchLogUtil.handleError("Usage: <BENCHNAME> <TOPIC> <MASTER> <BATCH_INTERVAL> <ZKHOST> <CONSUMER_GROUP> <KAFKA_THREADS>")
		}

		val benchName=args(0)
		val topic=args(1)
		val master=args(2)
		val batchInterval=args(3).toInt
		val zkHost=args(4)
		val consumerGroup=args(5)
		val kafkaThreads=args(6).toInt
		val recordCount=args(7).toLong
		val copies=args(8).toInt
		val testWAL=args(9).toBoolean
		val path=args(10)
		val debug=args(11).toBoolean

		val commonParamLength=12
		val param=ParamEntity(master,benchName,batchInterval,zkHost,consumerGroup,topic,kafkaThreads,recordCount,
			copies,testWAL,path,debug)
		val commonHint=" <TOPIC> <MASTER> <BATCH_INTERVAL> <ZKHOST> <CONSUMER_GROUP> <KAFKA_THREADS> "
	//Just for test
	if(benchName=="micro/projection"){
	  if(args.length<commonParamLength+2)
	    BenchLogUtil.handleError("Args needed "+benchName+"  <FIELD_INDEX> <SEPARATOR>")
	  val fieldIndex=args(commonParamLength).toInt
	  val separator=args(commonParamLength+1)
	  val SketchTest=new StreamProjectionJob(param,fieldIndex,separator)
	  SketchTest.run()
	}else if(benchName=="micro/sample"){
	  if(args.length<commonParamLength+1)
	    BenchLogUtil.handleError("Args needed "+benchName+commonHint+"<PROBABILITY>")
	    val prob=args(commonParamLength).toDouble
	    val SampleTest=new SampleStreamJob(param,prob)
	    SampleTest.run()
	}else if(benchName=="micro/statistics"){
	  if(args.length<commonParamLength+2)
	    BenchLogUtil.handleError("Args needed "+benchName+ commonHint+"<FIELD_INDEX> <SEPARATOR>")
	  val fieldIndex=args(commonParamLength).toInt
	  val separator=args(commonParamLength+1)
	  val numericCalc=new NumericCalcJob(param,fieldIndex,separator)
	  numericCalc.run()
	}else if(benchName=="micro/wordcount"){
	  if(args.length<commonParamLength+1)
	    BenchLogUtil.handleError("Args needed "+benchName+ commonHint+"<SEPARATOR>")
	    val separator=args(commonParamLength)
	    val wordCount=new Wordcount(param,separator)
	    wordCount.run()
	}else if(benchName=="micro/grep"){
	   if(args.length<commonParamLength+1)
	    BenchLogUtil.handleError("Args needed "+benchName+commonHint+"<PATTERN>")
	    val pattern=args(commonParamLength)
	    val GrepStream=new GrepStreamJob(param,pattern)
	   GrepStream.run()
	}else if(benchName=="micro/distinctcount"){
	   if(args.length<commonParamLength+2)
	    BenchLogUtil.handleError("Args needed "+benchName+ commonHint+"<FIELD_INDEX> <SEPARATOR>")
	   val fieldIndex=args(commonParamLength).toInt
	   val separator=args(commonParamLength+1)
	   val distinct=new DistinctCountJob(param,fieldIndex,separator)
	   distinct.run()
	}else{
	  val emptyTest=new IdentityJob(param)
	  emptyTest.run()
	}
	
	
  }
}
