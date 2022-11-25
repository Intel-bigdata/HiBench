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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.MetricsUtils

object ScalaDFSIOWriteOnly extends Logging {

  def config(uriStr: String): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.defaultFS", uriStr)
    conf
  }

  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println(
        s"Usage: $ScalaDFSIOWriteOnly <INPUT_HDFS> <OUTPUT_HDFS> <RD_NUM_OF_FILES> <RD_FILE_SIZE> <LINE_SIZE> <URI>"
      )
      System.exit(1)
    }
    logInfo("===========arguments[<INPUT_HDFS> <OUTPUT_HDFS> <RD_NUM_OF_FILES> <RD_FILE_SIZE> <LINE_SIZE> " +
      "<URI> ]============")
    args.foreach(logInfo(_))
    val nbrOfFiles = toLong(args(2), "RD_NUM_OF_FILES")
    val fileSize = toLong(args(3), "RD_FILE_SIZE")
    val totalSizeM = nbrOfFiles * fileSize
    val lineSize = toLong(args(4), "LINE_SIZE")
    val uri = args(5)
    val fileSizeByte = fileSize * 1024 * 1024
    if ((fileSizeByte % lineSize) != 0) {
      throw new IllegalArgumentException("line size, " + lineSize + ", should divide file size, " + fileSizeByte +
        ", exactly")
    }

    val sparkConf = new SparkConf().setAppName("ScalaDFSIOWriteOnly")
    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(config(uri))
    fs.mkdirs(new Path(args(0)))
    try {
      val data = sc.parallelize(Range(0, nbrOfFiles.toInt), nbrOfFiles.toInt)
      val readStart = System.currentTimeMillis()
      data.foreach(i => {
        val bytes = new Array[Byte](lineSize.toInt)
        Range(0, lineSize.toInt).foreach(i => bytes(i) = (i % 255).toByte)
        val fos = FileSystem.get(config(uri)).create(new Path(args(0) + "/data_" + i), true)
        try {
          var count = 0L
          while (count < fileSizeByte) {
            fos.write(bytes)
            count += bytes.length
          }
        } finally {
            fos.close()
        }
        MetricsUtils.setTaskWrite(fileSizeByte, fileSizeByte / lineSize)
      })
      val readEnd = System.currentTimeMillis()
      val dur = readEnd - readStart
      val durSec = dur/1000

      logInfo(s"===write [$totalSizeM(MB), $dur(ms)] perf: ${totalSizeM.toFloat/durSec}(MB/s) ")
    } finally {
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
