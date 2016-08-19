package com.intel.hibench.streambench.common.metrics

import java.util.concurrent.Callable

import com.codahale.metrics.Histogram
import kafka.consumer.SimpleConsumer

class FetchJob(zkConnect: String, topic: String, partition: Int,
    histogram: Histogram) extends Callable[FetchJobResult] {

  override def call(): FetchJobResult = {
    val result = new FetchJobResult()
    val consumer = new KafkaConsumer(zkConnect, topic, partition)
    while (consumer.hasNext) {
      val times = new String(consumer.next(), "UTF-8").split(":")
      val startTime = times(0).toLong
      val endTime = times(1).toLong
      histogram.update(endTime - startTime)
      result.update(startTime, endTime)
    }
    result
  }
}

class FetchJobResult(var minTime: Long, var maxTime: Long, var count: Long) {

  def this() = this(Long.MaxValue, Long.MinValue, 0)

  def update(startTime: Long ,endTime: Long): Unit = {
    count += 1

    if(startTime < minTime) {
      minTime = startTime
    }

    if(endTime > maxTime) {
      maxTime = endTime
    }
  }
}
