package org.apache.spark

import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag


class ConfigurableOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag,
                                      P <: Product2[K, V] : ClassTag]
      (self: RDD[P]) extends Logging with Serializable {
  private val ordering = implicitly[Ordering[K]]

  def sortByKeyWithPartitioner(ascending: Boolean = true,
                               partitioner: Partitioner): RDD[(K, V)] =
  {
    new ShuffledRDD[K, V, V](self, partitioner)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }
}
