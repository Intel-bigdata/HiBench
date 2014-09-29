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

package com.intel.sparkbench.terasort

import com.intel.sparkbench._
import org.apache.spark.rdd._
import org.apache.spark._

import scala.reflect.ClassTag

object ScalaTeraSort {
  implicit def rddToSampledOrderedRDDFunctions[K: Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        s"Usage: $ScalaTeraSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaTeraSort")
    val sc = new SparkContext(sparkConf)

    val file = sc.textFile(args(0))
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val data = file.map(line => (line.substring(0, 10), line.substring(10)))

    val partitioner = new BaseRangePartitioner(partitions = parallel / 2, rdd = data)
    val sorted_data = data.sortByKeyWithPartitioner(partitioner = partitioner).map { case (k, v) => k + v}
    sorted_data.saveAsTextFile(args(1))

    sc.stop()
  }
}
