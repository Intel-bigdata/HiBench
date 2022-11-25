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

package com.intel.sparkbench.micro

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.MetricsUtils

import scala.collection.mutable.ArrayBuffer

object ScalaDFSIOReadOnly extends Logging {

  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println(
        s"Usage: $ScalaDFSIOE <INPUT_HDFS> <OUTPUT_HDFS> <RD_NUM_OF_FILES> <RD_FILE_SIZE> <LINE_SIZE> <URI>"
      )
      System.exit(1)
    }
    logInfo("===========arguments[<INPUT_HDFS> <OUTPUT_HDFS> <RD_NUM_OF_FILES> <RD_FILE_SIZE> <LINE_SIZE> " +
      "<URI>] ============")
    args.foreach(logInfo(_))
    val lineSize = toLong(args(4), "LINE_SIZE")
    val nbrOfFiles = toLong(args(2), "RD_NUM_OF_FILES")
    val fileSize = toLong(args(3), "RD_FILE_SIZE")
    val totalSizeM = nbrOfFiles * fileSize
    val uri = args(5)

    val sparkConf = new SparkConf().setAppName("ScalaDFSIOReadOnly")
    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(ScalaDFSIOWriteOnly.config(uri))
    val folder = args(0)
    val files = fs.listFiles(new Path(folder), false)
    val paths = ArrayBuffer[String]()
    while (files.hasNext()) {
      paths += files.next().getPath().toString
    }
    try {
      val data = sc.parallelize(paths, paths.size)
      val readStart = System.currentTimeMillis()
      val fileSizeByte = fileSize * 1024 * 1024
      data.foreach(path => {
        val fos = FileSystem.get(ScalaDFSIOWriteOnly.config(uri)).open(new Path(path))
        try {
          var count = 0L
          var size = 1
          val bytes = new Array[Byte](lineSize.toInt)
          while (size > 0 & count < fileSizeByte) {
            size = fos.read(bytes)
            count += size
          }
        } finally {
          fos.close()
        }
        MetricsUtils.setTaskRead(fileSizeByte, fileSizeByte / lineSize)
      })
      val readEnd = System.currentTimeMillis()
      val dur = readEnd - readStart
      val durSec = dur/1000

      logInfo(s"===read [$totalSizeM(MB), $dur(ms)] perf: ${totalSizeM.toFloat/durSec}(MB/s) ")
    } finally {
      if (fs != null ) {
        fs.close()
      }
      sc.stop()
    }
  }

  // More hints on Exceptions
  private def toBoolean(str: String, parameterName: String): Boolean = {
    try {
      str.toBoolean
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
        s"Unrecognizable parameter ${parameterName}: ${str}, should be true or false")
    }
  }

  private def toLong(str: String, parameterName: String): Long = {
    try {
      str.toLong
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Unrecognizable parameter ${parameterName}: ${str}, should be integer")
    }
  }

}
