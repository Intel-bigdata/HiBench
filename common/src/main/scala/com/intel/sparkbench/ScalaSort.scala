// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.intel.sparkbench.sort

import org.apache.spark.rdd._
import org.apache.spark._
import scala.reflect.ClassTag


class HashedRDDFunctions[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag](self: RDD[P])
  extends Logging with Serializable {

  private val ordering = implicitly[Ordering[K]]

  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[P] = {
    println("in custom sort by key")
    val part = new HashPartitioner(numPartitions)
    val shuffled = new ShuffledRDD[K, V, P](self, part)
    shuffled.mapPartitions(iter => {
      val buf = iter.toArray
      if (ascending) {
        buf.sortWith((x, y) => ordering.lt(x._1, y._1)).iterator
      } else {
        buf.sortWith((x, y) => ordering.gt(x._1, y._1)).iterator
      }
    }, preservesPartitioning = true)
  }
}

object ScalaSort{
  implicit def rddToHashedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)]) =
    new HashedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaSort")
    val sc = new SparkContext(sparkConf)

    val file = sc.textFile(args(0))
    val counts = file.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .sortByKey()
    counts.saveAsTextFile(args(1))
    sc.stop()
  }
}
