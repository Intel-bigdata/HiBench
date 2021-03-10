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

import org.apache.hadoop.examples.terasort.TeraInputFormat
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

object ScalaDFSIOE extends Logging {

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        s"Usage: $ScalaDFSIOE <INPUT_HDFS> <OUTPUT_HDFS> <RD_NUM_OF_FILES> <RD_FILE_SIZE> <READ_ONLY>"
      )
      System.exit(1)
    }
    logInfo("===========arguments[<INPUT_HDFS> <OUTPUT_HDFS> <RD_NUM_OF_FILES> <RD_FILE_SIZE> <READ_ONLY>]" +
      "============")
    args.foreach(logInfo(_))
    val readOnly = toBoolean(args(4), "READ_ONLY")
    val nbrOfFiles = toLong(args(2), "RD_NUM_OF_FILES")
    val fileSize = toLong(args(3), "RD_FILE_SIZE")
    val totalSizeM = nbrOfFiles * fileSize

    val sparkConf = new SparkConf().setAppName("ScalaDFSIOE")
    val sc = new SparkContext(sparkConf)
    try {
      val data = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](args(0) + "/io_data").map {
        case (k, v) => k.copyBytes() ++ v.copyBytes
      }

      if (!readOnly) {
        data.persist(StorageLevel.MEMORY_ONLY_SER)
      }
      val readStart = System.currentTimeMillis()
      data.foreach(_ => ())
      val readEnd = System.currentTimeMillis()
      val dur = readEnd - readStart
      val durSec = dur/1000

      logInfo(s"===read [$totalSizeM(MB), $dur(ms)] perf: ${totalSizeM.toFloat/durSec}(MB/s) ")
      if (!readOnly) {
        val writeStart = System.currentTimeMillis()
        data.map(array => (NullWritable.get(), new BytesWritable(array)))
          .saveAsNewAPIHadoopFile(args(1), classOf[NullWritable], classOf[BytesWritable],
            classOf[MapFileOutputFormat])
        val writeEnd = System.currentTimeMillis()
        val durWrite = writeEnd - writeStart
        val durWriteSec = durWrite/1000
        logInfo(s"===write [$totalSizeM(MB), $durWrite(ms)] perf: ${totalSizeM.toFloat/durWriteSec}(MB/s) ")
      }
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
