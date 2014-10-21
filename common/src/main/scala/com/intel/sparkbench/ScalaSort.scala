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

package com.intel.sparkbench.sort

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkBench.IOCommon
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel._

import scala.reflect.runtime.universe


object ScalaSort{

  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaSort")
    val sc = new SparkContext(sparkConf)
    val parallel = sc.getConf.getInt("spark.default.parallelism", 0)
    val input_format = sc.getConf.get("sparkbench.inputformat", "TextInputFormat")
    val output_format = sc.getConf.get("sparkbench.outputformat", "TextOutputFormat")
    val input_format_codec = sc.getConf.get("sparkbench.inputformat.codec",
                                            "org.apache.hadoop.io.compress.SnappyCodec")
    val output_format_codec = sc.getConf.get("sparkbench.outputformat.codec",
                                             "org.apache.hadoop.io.compress.SnappyCodec")


    val io = new IOCommon(sc)
    val data = io.load(args(0))
    val sorted = data
      .flatMap(_.split(" "))
      .mapPartitions(_.toList.sorted.toIterator,
                     preservesPartitioning = true)

    io.save(args(1), sorted)

    sc.stop()
  }
}
