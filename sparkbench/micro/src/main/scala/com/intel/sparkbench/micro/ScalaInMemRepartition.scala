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

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark._
import org.apache.spark.rdd.{MemoryDataRDD, PairRDDFunctions, RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import java.util.Random
import scala.util.hashing
import scala.compat.java8.FunctionConverters._

object ScalaInMemRepartition {

  val localData = Range(0, 200).map(i => i.toByte).toArray
  val local = ThreadLocal.withInitial[Array[Byte]](
    (() => localData.clone).asJava
    )

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        s"Usage: $ScalaInMemRepartition <NBR_OF_RECORD> <OUTPUT_HDFS> <CACHE_IN_MEMORY> <DISABLE_OUTPUT>"
      )
      System.exit(1)
    }
    val cache = toBoolean(args(2), ("CACHE_IN_MEMORY"))
    val nbrOfRecords = toInt(args(0), ("NBR_OF_RECORD"))
    val outputDir = args(1)
    val disableOutput = toBoolean(args(3), ("DISABLE_OUTPUT"))

    val sparkConf = new SparkConf().setAppName("ScalaInMemRepartition")
    val sc = new SparkContext(sparkConf)

    val mapParallelism = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val reduceParallelism  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((mapParallelism / 2).toString).toInt

    // val sleepDur = if (cache) 3000 else 0
    val data = new MemoryDataRDD(sc, mapParallelism, nbrOfRecords, 200)
    // data.persist(StorageLevel.MEMORY_ONLY)
    //data.foreach(_ => {})

    val paired: PairRDDFunctions[Int, Array[Byte]] = data.mapPartitionsWithIndex {
      val nbrOfReduces = reduceParallelism
      (index, part) => {
        var position = new Random(hashing.byteswap32(index)).nextInt(nbrOfReduces)
        part.map { row => {
          position = position + 1
          (position, row)
        }
        }
      }
    }

    


    val shuffled = paired.partitionBy(new HashPartitioner(reduceParallelism))
    if (disableOutput) {
      shuffled.foreach(_ => {})
    } else {
      shuffled.map { case (_, v) => (NullWritable.get(), new Text(v)) }
        .saveAsNewAPIHadoopFile[TextOutputFormat[NullWritable, Text]](outputDir)
    }

    sc.stop()
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

  private def toInt(str: String, parameterName: String): Int = {
    try {
      str.toInt
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Unrecognizable parameter ${parameterName}: ${str}, should be integer")
    }
  }
}
