package com.intel.hibench.streambench.common

import java.io.{PrintWriter, File}

/**
  * N.B. This file is just a temp workaround during script developing. Remove it when release!!!
  *
  * This logger will add messages into given path.
  */
class TempLogger(logPath: String, platForm: Platform.Platform, testCase: TestCase.TestCase)
  extends Logger(platForm, testCase){

  val file = new File(logPath)
  val out = new PrintWriter(file)

  override def logThroughput(throughput: String) = {
    val msg = s"${prefix}: Throughput: ${throughput}"
    logMsg(msg)
  }

  override def logAvgLatency (time: String) = {
    val msg =s"${prefix}: Average Latency: ${time}"
    logMsg(msg)
  }

  override def logLatency(recordNum: Int, time: String) = {
    val msg = s"${prefix}: Latency: ${time} of ${recordNum} records"
    logMsg(msg)
  }

  override def logConfig(name: String, value: String) = {
    val msg = s"${prefix}: Config: ${name}=${value}"
    logMsg(msg)
  }

  override def logMsg(msg:String) {
    out.println(msg)
    out.flush()
    System.out.println(msg)
  }

  def handleError(msg:String){
    System.err.println(msg)
    System.exit(1)
  }
}
