/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.streambench.spark.util

import org.apache.log4j.{Level, Logger}
import com.intel.hibench.streambench.spark.RunBench

object BenchLogUtil {
  def setLogLevel(){
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
	if (!log4jInitialized) {
	  println("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
	}
  }
  
  val file=new java.io.File(RunBench.reportDir + "/streamingbench/spark/streambenchlog.txt")
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
