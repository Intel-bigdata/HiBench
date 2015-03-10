package com.intel.PRCcloud.streamBench.util

import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}

object BenchLogUtil extends Logging{
  def setLogLevel(){
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
	if (!log4jInitialized) {
	  logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
	}
  }
  
  val file=new java.io.File("/tmp/benchlog.txt")
  val out=new java.io.PrintWriter(file)
  
  def logMsg(msg:String) {
    out.println(msg)
    out.flush()
    System.out.println(msg)
  }
  
  def handleError(msg:String){
    System.err.println(msg)
    System.exit(1)
  }
}