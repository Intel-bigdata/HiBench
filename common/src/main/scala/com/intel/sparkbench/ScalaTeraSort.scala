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

import org.apache.spark.rdd._
import org.apache.spark._
import scala.reflect.ClassTag

class SampleRangePartitioner [K : Ordering : ClassTag, V](
         @transient partitions: Int,
         @transient rdd: RDD[_ <: Product2[K,V]],
         private var ascending: Boolean = true)
  extends RangePartitioner[K, V](partitions, rdd, ascending) {


  }
class SampledOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag,
                         P <: Product2[K, V] : ClassTag]
                        (self: RDD[P]) extends Logging with Serializable {
  private val ordering = implicitly[Ordering[K]]

  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[(K, V)] =
  {
    val part = new RangePartitioner(numPartitions, self, ascending)
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }
}

object ScalaTeraSort{
  implicit def rddToSampledOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag]
     (rdd: RDD[(K, V)]) = new SampledOrderedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]){
    if (args.length != 2){
      System.err.println(
        s"Usage: $ScalaTeraSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaTeraSort")
    val sc = new SparkContext(sparkConf)

    val file = sc.textFile(args(0))
    val parallel = sc.getConf.getInt("spark.default.parallelism", 0)
    val data = file.map(line => (line.substring(0, 10), line.substring(10)))
      .sortByKey(numPartitions=parallel).map{case(k, v) => k + v}
    data.saveAsTextFile(args(1))

    sc.stop()
  }
}
