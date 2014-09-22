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

import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDDLike}
import org.apache.spark.rdd._
import org.apache.spark._
import scala.reflect.ClassTag

class HashedRDDFunctions[K : Ordering : ClassTag,
                         V: ClassTag,
                         P <: Product2[K, V] : ClassTag]
                        (self: RDD[P])
  extends Logging with Serializable
{
  private val ordering = implicitly[Ordering[K]]

  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[(K, V)] =
  {
    val part = new HashPartitioner(numPartitions)
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }
}

class PatchedJavaPairRDD[K, V](override val rdd: RDD[(K, V)])
                       (implicit override val kClassTag: ClassTag[K], implicit override val vClassTag: ClassTag[V])
  extends JavaPairRDD[K, V](rdd) {
  def sortByKeyWithHashedPartitioner(): JavaPairRDD[K, V] = sortByKeyWithHashedPartitioner(true)

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKeyWithHashedPartitioner(ascending: Boolean): JavaPairRDD[K, V] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[K]]
    sortByKeyWithHashedPartitioner(comp, ascending)
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKeyWithHashedPartitioner(ascending: Boolean, numPartitions: Int): JavaPairRDD[K, V] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[K]]
    sortByKeyWithHashedPartitioner(comp, ascending, numPartitions)
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKeyWithHashedPartitioner(comp: Comparator[K]): JavaPairRDD[K, V] = sortByKeyWithHashedPartitioner(comp, true)

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKeyWithHashedPartitioner(comp: Comparator[K], ascending: Boolean): JavaPairRDD[K, V] = {
    implicit val ordering = comp // Allow implicit conversion of Comparator to Ordering.
    fromRDD(new HashedRDDFunctions[K, V, (K, V)](rdd).sortByKey(ascending))
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKeyWithHashedPartitioner(comp: Comparator[K], ascending: Boolean, numPartitions: Int): JavaPairRDD[K, V] = {
    implicit val ordering = comp // Allow implicit conversion of Comparator to Ordering.
    fromRDD(new HashedRDDFunctions[K, V, (K, V)](rdd).sortByKey(ascending, numPartitions))
  }

}


object ScalaSort{
  implicit def rddToHashedRDDFunctions[K : Ordering : ClassTag, V: ClassTag]
         (rdd: RDD[(K, V)]) = new HashedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]){
    if (args.length != 3){
      System.err.println(
        s"Usage: $ScalaSort <INPUT_HDFS> <OUTPUT_HDFS> <PARALLEL>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaSort")
    val sc = new SparkContext(sparkConf)
    val parallel = args(2).toInt

    val file = sc.textFile(args(0))
    val data = file.flatMap(line => line.split(" ")).map((_, 1))
    val sorted = data.sortByKey(numPartitions = parallel / 2).map(_._1)

    sorted.saveAsTextFile(args(1))
    sc.stop()
  }
}
