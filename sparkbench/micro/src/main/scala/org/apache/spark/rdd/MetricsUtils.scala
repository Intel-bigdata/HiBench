package org.apache.spark.rdd

import org.apache.spark.TaskContext

object MetricsUtils {

  def setTaskRead(bytesRead: Long, recordsRead: Long): Unit = {
    val tc = TaskContext.get()
    tc.taskMetrics().inputMetrics.setBytesRead(bytesRead)
    tc.taskMetrics().inputMetrics.incRecordsRead(recordsRead)
  }

  def setTaskWrite(bytesWrite: Long, recordsWrite: Long): Unit = {
    val tc = TaskContext.get()
    tc.taskMetrics().outputMetrics.setBytesWritten(bytesWrite)
    tc.taskMetrics().outputMetrics.setRecordsWritten(recordsWrite)
  }
}
