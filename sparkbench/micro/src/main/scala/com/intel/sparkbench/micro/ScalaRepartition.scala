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

package com.intel.hibench.sparkbench.micro

import java.util.Random

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.hadoop.examples.terasort.TeraInputFormat
import org.apache.hadoop.io.Text
import org.apache.spark._
import org.apache.spark.rdd.{CoalescedRDD, RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel

object ScalaRepartition {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        s"Usage: $ScalaRepartition <INPUT_HDFS> <OUTPUT_HDFS> <CACHE_IN_MEMORY>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaRepartition")
    val sc = new SparkContext(sparkConf)

    val data = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](args(0)).map {
      case (k,v) => k.copyBytes ++ v.copyBytes
    }

    if (args(2) == "true") {
      data.persist(StorageLevel.MEMORY_ONLY)
      data.count()
    } else if (args(2) != "false") {
      throw new IllegalArgumentException(
        s"Unrecognizable parameter CACHE_IN_MEMORY: ${args(2)}, should be true or false")
    }

    val mapParallelism = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val reduceParallelism  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((mapParallelism / 2).toString).toInt

    reparition(data, reduceParallelism).foreach(_ => {})

    sc.stop()
  }

  // Save a CoalescedRDD than RDD.repartition API
  private def reparition(previous: RDD[Array[Byte]], numReducers: Int): ShuffledRDD[Int, Array[Byte], Array[Byte]] = {
    /** Distributes elements evenly across output partitions, starting from a random partition. */
    val distributePartition = (index: Int, items: Iterator[Array[Byte]]) => {
      var position = (new Random(index)).nextInt(numReducers)
      items.map { t =>
        // Note that the hash code of the key will just be the key itself. The HashPartitioner
        // will mod it with the number of total partitions.
        position = position + 1
        (position, t)
      }
    } : Iterator[(Int, Array[Byte])]

    // include a shuffle step so that our upstream tasks are still distributed
    new ShuffledRDD[Int, Array[Byte], Array[Byte]](previous.mapPartitionsWithIndex(distributePartition),
      new HashPartitioner(numReducers))
  }

}
