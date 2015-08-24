package com.intel.hibench.streambench.spark

import com.intel.hibench.streambench.spark.entity.ParamEntity
import com.intel.hibench.streambench.spark.util._
import com.intel.hibench.streambench.spark.microbench._

object RunBench {
	def main(args: Array[String]) {
		this.run(args)
	}

	def run(args: Array[String]) {
		if (args.length < 1) {
			BenchLogUtil.handleError("Usage: RunBench <ConfigFile>")
		}

		val conf = new ConfigLoader(args(0))

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
