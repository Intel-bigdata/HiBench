package com.intel.hibench.gearpumpbench.task

import com.intel.hibench.common.streaming.metrics.KafkaReporter
import com.intel.hibench.gearpumpbench.util.GearpumpConfig
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}

import scala.collection.mutable

class Sum(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private val benchConfig = conf.getValue[GearpumpConfig](GearpumpConfig.BENCH_CONFIG).get
  val reporter = new KafkaReporter(benchConfig.reporterTopic, benchConfig.brokerList)
  private val map: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  override def onNext(msg: Message): Unit = {
    if (null != msg) {
      val current = map.getOrElse(msg.msg.asInstanceOf[String], 0L)
      map.put(msg.msg.asInstanceOf[String], current + 1)
      reporter.report(msg.timestamp, System.currentTimeMillis())
    }
  }
}