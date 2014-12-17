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

package com.intel.sparkbench.datagen.convert

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.mahout.math.VectorWritable
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object KmeansConvert{
  val conf = new Configuration()
  def main(args: Array[String]) {
    if ( args.length != 2 ) {
      System.err.println("Usage: Convert <input_directory> <output_file_path>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HiBench KMeans Converter")
    val sc = new SparkContext(sparkConf)

    val input_path  = args(0) //"hdfs://localhost:54310/SparkBench/KMeans/Input/samples/"
    val output_name = args(1) //"/HiBench/KMeans/Input/samples.txt"
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)

    val data = sc.sequenceFile[LongWritable, VectorWritable](input_path)
    data.repartition(parallel)
    data.map { case (k, v) =>
      var vector: Array[Double] = new Array[Double](v.get().size)
      for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
      vector.mkString(" ")
    }.saveAsTextFile(output_name)
    sc.stop()
  }
}
