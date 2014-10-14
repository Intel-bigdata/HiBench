package org.apache.spark

import java.util.Comparator

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag


class ConfigurableOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag,
                                      P <: Product2[K, V] : ClassTag]
      (self: RDD[P]) extends Logging with Serializable {
  private val ordering = implicitly[Ordering[K]]

  def sortByKeyWithPartitioner(partitioner: Partitioner,
                               ascending: Boolean = true): RDD[(K, V)] =
  {
    new ShuffledRDD[K, V, V](self, partitioner)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }
}


class ConfigurableJavaPairRDD[K, V](override val rdd: RDD[(K, V)], val kClass: Class[K], val vClass: Class[V])
  extends JavaPairRDD[K, V](rdd)(ClassTag(kClass), ClassTag(vClass)) {
  implicit val keyCmt: ClassTag[K] = ClassTag(kClass)
  implicit val keyCmv: ClassTag[V] = ClassTag(vClass)
  def sortByKeyWithPartitioner(partitioner: Partitioner): JavaPairRDD[K, V] =
    sortByKeyWithPartitioner(partitioner, true)

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKeyWithPartitioner(partitioner: Partitioner, ascending: Boolean): JavaPairRDD[K, V] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[K]]
    sortByKeyWithPartitioner(partitioner, comp, ascending)
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKeyWithPartitioner(partitioner: Partitioner, comp: Comparator[K]): JavaPairRDD[K, V] =
    sortByKeyWithPartitioner(partitioner, comp, true)

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKeyWithPartitioner(partitioner: Partitioner, comp: Comparator[K], ascending: Boolean): JavaPairRDD[K, V] = {
    implicit val ordering = comp // Allow implicit conversion of Comparator to Ordering.
    fromRDD(new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd).sortByKeyWithPartitioner(partitioner, ascending))
  }

}