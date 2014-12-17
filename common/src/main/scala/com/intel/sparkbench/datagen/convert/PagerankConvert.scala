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
import org.apache.spark.{SparkConf, SparkContext}

object PagerankConvert{
  val conf = new Configuration()
  def main(args: Array[String]){
    if (args.length != 2){
      System.err.println("Usage: Convert <input_directory> <output_file_path>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HiBench PageRank Converter")
    val sc = new SparkContext(sparkConf)

    val input_path  = args(0)  //"/HiBench/Pagerank/Input/edges"
    val output_name = args(1) // "/HiBench/Pagerank/Input/edges.txt"
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)

    val data = sc.textFile(input_path).map{case(line)=>
        val elements = line.split('\t')
        val elements_tuple = elements.slice(elements.length-2, elements.length)
        "%s  %s".format(elements_tuple(0), elements_tuple(1))
    }

    data.repartition(parallel)
    data.saveAsTextFile(output_name)
    sc.stop()
  }
}
