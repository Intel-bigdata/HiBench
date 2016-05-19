package com.intel.hibench.streambench.gearpump.task

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}

import scala.collection.mutable

class Sum(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private val map: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  override def onNext(msg: Message): Unit = {
    if (null != msg) {
      val current = map.getOrElse(msg.msg.asInstanceOf[String], 0L)
      map.put(msg.msg.asInstanceOf[String], current + 1)
    }
  }
}