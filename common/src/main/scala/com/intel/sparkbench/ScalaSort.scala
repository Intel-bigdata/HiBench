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

import java.util.Comparator

import org.apache.spark._
import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDDLike}
import org.apache.spark.rdd._

import scala.reflect.ClassTag

object ScalaSort{
  implicit def rddToHashedRDDFunctions[K : Ordering : ClassTag, V: ClassTag]
         (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]){
    if (args.length != 2){
      System.err.println(
        s"Usage: $ScalaSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaSort")
    val sc = new SparkContext(sparkConf)
    val reducer  = System.getProperties.getProperty("sparkbench.reducer").toInt
    val file = sc.textFile(args(0))
    val data = file.flatMap(line => line.split(" ")).map((_, 1))
    val sorted = data.sortByKeyWithPartitioner(partitioner = new HashPartitioner(reducer)).map(_._1)

    sorted.saveAsTextFile(args(1))
    sc.stop()
  }
}
