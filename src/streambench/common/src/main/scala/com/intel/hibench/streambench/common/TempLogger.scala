package com.intel.hibench.streambench.common

import java.io.{File, PrintWriter}

/**
 * N.B. This file is just a temp workaround during script developing. Remove it when release!!!
 *
 * This logger will add messages into given path.
 */
class TempLogger(logPath: String, platForm: Platform, testCase: TestCase.TestCase)
    extends Logger(platForm, testCase) {

  val file = new File(logPath)
  val out = new PrintWriter(file)

  override def logThroughput(throughput: String) = {
    val msg = s"${prefix}: Throughput: ${throughput} records/s"
    doLog(msg)
  }

  override def logAvgLatency(time: String) = {
    val msg = s"${prefix}: Average Latency: ${time} ms"
    doLog(msg)
  }

  override def logLatency(recordNum: Long, time: String) = {
    val msg = s"${prefix}: Latency: ${time} ms of ${recordNum} records"
    doLog(msg)
  }

  override def logConfig(name: String, value: String) = {
    val msg = s"${prefix}: Config: ${name}=${value}"
    doLog(msg)
  }

  override def logMsg(msg0: String) = {
    val msg = s"${prefix}: Message: ${msg0}"
    doLog(msg)
  }

  def doLog(msg: String) {
    out.println(msg)
    out.flush()
    System.out.println(msg)
  }

  def handleError(msg: String) {
    System.err.println(msg)
    System.exit(1)
  }
}
