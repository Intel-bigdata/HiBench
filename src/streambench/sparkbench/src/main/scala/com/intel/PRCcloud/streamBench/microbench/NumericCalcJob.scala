package com.intel.PRCcloud.streamBench.microbench

import com.intel.PRCcloud.streamBench.entity.ParamEntity
import org.apache.spark.streaming.dstream.DStream
import com.intel.PRCcloud.streamBench.metrics.LatencyListener
import org.apache.spark.streaming.StreamingContext
import com.intel.PRCcloud.streamBench.util.BenchLogUtil

case class MultiReducer(var max: Long, var min: Long, var sum: Long, var count: Long) extends Serializable {
  def this() = this(0, Int.MaxValue, 0, 0)

  def reduceValue(value: Long): MultiReducer = {
    this.max = Math.max(this.max, value)
    this.min = Math.min(this.min, value)
    this.sum += value
    this.count += 1
    this
  }

  def reduce(that: MultiReducer): MultiReducer = {
    this.max = Math.max(this.max, that.max)
    this.min = Math.min(this.min, that.min)
    this.sum += that.sum
    this.count += that.count
    this
  }
}

class NumericCalcJob(subClassParams: ParamEntity, fieldIndex: Int, separator: String)
  extends RunBenchJobWithInit(subClassParams) {
  class Aggregator(val ValMin:Long, val ValMax:Long, val ValSum:Long, val ValCount:Long) {
    def aggr(v:Aggregator) = {
      val vmin = Math.min(ValMin, v.ValMin)
      val vmax = Math.max(ValMax, v.ValMax)
      val vsum = ValSum + v.ValSum
      val vcount = ValCount + v.ValCount

      new Aggregator(vmin, vmax, vsum, vcount)
    }
  }

  var history_statistics = new MultiReducer()

  override def processStreamData(lines: DStream[String], ssc: StreamingContext) {
    val index = fieldIndex
    val sep = separator

    lines.foreachRDD( rdd => {
      val numbers = rdd.flatMap( line => {
        val splits = line.split(sep)
        if (index < splits.length)
          Iterator(splits(index).toLong)
        else
          Iterator.empty
      })

      var zero = new MultiReducer()
      val cur = numbers.map(x => new MultiReducer(x, x, x, 1))
        .fold(zero)((v1, v2) => v1.reduce(v2))
      //var cur = numbers.aggregate(zero)((v, x) => v.reduceValue(x), (v1, v2) => v1.reduce(v2))
      history_statistics.reduce(cur)

      BenchLogUtil.logMsg("Current max: " + history_statistics.max)
      BenchLogUtil.logMsg("Current min: " + history_statistics.min)
      BenchLogUtil.logMsg("Current sum: " + history_statistics.sum)
      BenchLogUtil.logMsg("Current total: " + history_statistics.count)
      BenchLogUtil.logMsg("Current avg: " + (history_statistics.sum.toDouble / history_statistics.count.toDouble))

    })
  }
}

