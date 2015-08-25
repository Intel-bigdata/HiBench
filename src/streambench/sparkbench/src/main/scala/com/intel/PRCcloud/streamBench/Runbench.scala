package com.intel.PRCcloud.streamBench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import com.intel.PRCcloud.streamBench.util._
import com.intel.PRCcloud.streamBench.microbench._

object RunBench {
	def main(args: Array[String]) {
		//val args2=Array("micro/identity","spark_test_1_1","spark://sr119:7077","1","sr464:2181","spark_consumer_1","1","100000")
		this.run(args)
	}

	def run(args: Array[String]) {
		//		if(args.length < 8){
		//			BenchLogUtil.handleError("Usage: <BENCHNAME> <TOPIC> <MASTER> <BATCH_INTERVAL> <ZKHOST> <CONSUMER_GROUP> <KAFKA_THREADS>")
		//		}
		if (args.length < 1) {
			BenchLogUtil.handleError("Usage: RunBench <ConfigFile>")
		}

		val conf = new ConfigLoader(args(0))
		//		val benchName=args(0)
		//		val topic=args(1)
		//		val master=args(2)
		//		val batchInterval=args(3).toInt
		//		val zkHost=args(4)
		//		val consumerGroup=args(5)
		//		val kafkaThreads=args(6).toInt
		//		val recordCount=args(7).toLong
		//		val copies=args(8).toInt
		//		val testWAL=args(9).toBoolean
		//		var path=""
		//		var nexti=10
		//		if (testWAL) {
		//						path=args(10)
		//						nexti=11
		//		}
		//
		//		val debug = args(nexti).toBoolean
		//		val directMode = args(nexti+1).toBoolean
		//var brokerList = ""
		//		if (directMode) {
		//			brokerList = args(nexti+2)
		//		}

		val benchName = conf.getPropertiy("hibench.streamingbench.benchname")
		val topic = conf.getPropertiy("hibench.streamingbench.topic_name")
		val master = conf.getPropertiy("hibench.spark.master")
		val batchInterval = conf.getPropertiy("hibench.streamingbench.batch_interval").toInt
		val zkHost = conf.getPropertiy("hibench.streamingbench.zookeeper.host")
		val consumerGroup = conf.getPropertiy("hibench.streamingbench.consumer_group")
		val kafkaThreads = conf.getPropertiy("hibench.streamingbench.receiver_nodes").toInt
		val recordCount = conf.getPropertiy("hibench.streamingbench.record_count").toLong
		val copies = conf.getPropertiy("hibench.streamingbench.copies").toInt
		val testWAL = conf.getPropertiy("hibench.streamingbench.testWAL").toBoolean
		val path = if (testWAL) conf.getPropertiy("hibench.streamingbench.checkpoint_path") else ""
		val debug = conf.getPropertiy("hibench.streamingbench.debug").toBoolean
		val directMode = conf.getPropertiy("hibench.streamingbench.direct_mode").toBoolean
		val brokerList = if (directMode) conf.getPropertiy("hibench.streamingbench.brokerList") else ""


		val param = ParamEntity(master, benchName, batchInterval, zkHost, consumerGroup, topic, kafkaThreads, recordCount, copies, testWAL, path, debug, directMode, brokerList)
    benchName match {
      case "micro-projection" =>
        val fieldIndex = conf.getPropertiy("hibench.streamingbench.field_index").toInt
        val separator = conf.getPropertiy("hibench.streamingbench.separator")
        val ProjectTest = new StreamProjectionJob(param, fieldIndex, separator)
        ProjectTest.run()
      case "micro-sample" =>
        val prob = conf.getPropertiy("hibench.streamingbench.prob").toDouble
        val SampleTest = new SampleStreamJob(param, prob)
        SampleTest.run()
      case "micro-statistics" =>
        val fieldIndex = conf.getPropertiy("hibench.streamingbench.field_index").toInt
        val separator = conf.getPropertiy("hibench.streamingbench.separator")
        val numericCalc = new NumericCalcJob(param, fieldIndex, separator)
        numericCalc.run()
      case "micro-wordcount" =>
        val separator = conf.getPropertiy("hibench.streamingbench.separator")
        val wordCount = new Wordcount(param, separator)
        wordCount.run()
      case "micro-grep" =>
        val pattern = conf.getPropertiy("hibench.streamingbench.pattern")
        val GrepStream = new GrepStreamJob(param, pattern)
        GrepStream.run()
      case "micro-distinctcount" =>
        val fieldIndex = conf.getPropertiy("hibench.streamingbench.field_index").toInt
        val separator = conf.getPropertiy("hibench.streamingbench.separator")
        val distinct = new DistinctCountJob(param, fieldIndex, separator)
        distinct.run()
      case _ =>
        val emptyTest = new IdentityJob(param)
        emptyTest.run()
		}


	}
}
