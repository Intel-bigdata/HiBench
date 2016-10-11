package com.intel.hibench.sparkbench.graph.nweight

import it.unimi.dsi.fastutil.objects.ObjectHeaps

class SizedPriorityQueue(
  val capacity:Int) extends Traversable[(Long, Double)] with Serializable {
  protected val buf = new Array[(Long, Double)](capacity)
  protected val comparator = new java.util.Comparator[(Long, Double)] with Serializable {
    override def compare(m1: (Long, Double), m2: (Long, Double)) = if (m1._2 < m2._2) -1 else (if (m1._2 > m2._2) 1 else (if (m1._1 < m2._1) -1 else (if (m1._1 > m2._1) 1 else 0)))
  }

  protected var size_ = 0

  override def size() = size_

  def clear() {
    size_ = 0
  }

  def fullySorted(): Array[(Long, Double)] = {
    val slicedBuf = buf.slice(0, size_ - 1)
    java.util.Arrays.sort(slicedBuf, comparator)
    slicedBuf
  }

  def foreach[U](f: ((Long, Double)) => U): Unit = {
    for (i <- 0 until size_) f(buf(i))
  }

  def enqueue(value: (Long, Double)) {
    if (size_ < capacity) {
      buf(size_) = value
      size_ = size_ + 1
      ObjectHeaps.upHeap(buf, size_, size_ - 1, comparator)
    } else if (comparator.compare(value, buf(0)) > 0) {
      buf(0) = value
      ObjectHeaps.downHeap(buf, size_, 0, comparator)
    }
  }

}

object SizedPriorityQueue {
  def apply(capacity :Int)(elems: (Long, Double)*) = {
    val q = new SizedPriorityQueue(capacity);
    for ((i, v) <- elems)
      q.enqueue(i, v);
    q
  }
}
