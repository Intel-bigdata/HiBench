package com.intel.hibench.streambench.gearpump.task

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}

class NumericTogether(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private var max: Long = 0
  private var min: Long = Long.MaxValue
  private var sum: Long = 0
  private var count: Long = 0

  override def onNext(msg: Message): Unit = {
    val (_max, _min, _sum, _count) = msg.msg.asInstanceOf[(Long, Long, Long, Long)]
    if (_max > max) max = _max
    if (_min < min) min = _min
    sum += _sum
    count += _count

    val average = sum.toDouble / count.toDouble
    //LOG out
  }
}
