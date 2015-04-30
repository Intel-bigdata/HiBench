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

import com.intel.sparkbench.IOCommon
import org.apache.hadoop.examples.terasort.{TeraInputFormat,TeraOutputFormat}
import org.apache.hadoop.io.Text
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

object ScalaTeraSort {
  implicit def rddToSampledOrderedRDDFunctions[K: Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  implicit def ArrayByteOrdering: Ordering[Array[Byte]] = Ordering.fromLessThan{case (a, b)=> a.compareTo(b)<0}
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        s"Usage: $ScalaTeraSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaTeraSort")
    val sc = new SparkContext(sparkConf)
    val io = new IOCommon(sc)

    //val file = io.load[String](args(0), Some("Text"))
    val data = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](args(0)).map{case (k,v)=>(k.getBytes, v.getBytes)}
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val reducer  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    val partitioner = new BaseRangePartitioner(partitions = reducer, rdd = data)
    val ordered_data = new ConfigurableOrderedRDDFunctions[Array[Byte], Array[Byte], (Array[Byte], Array[Byte])](data)
    val sorted_data = ordered_data.sortByKeyWithPartitioner(partitioner = partitioner).map{case (k, v)=>(new Text(k), new Text(v))}

    sorted_data.saveAsNewAPIHadoopFile[TeraOutputFormat](args(1))
    //io.save(args(1), sorted_data)


    sc.stop()
  }
}
