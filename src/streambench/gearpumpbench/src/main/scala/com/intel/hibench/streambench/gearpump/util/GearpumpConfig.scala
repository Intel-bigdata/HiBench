package com.intel.hibench.streambench.gearpump.util

case class GearpumpConfig(
  benchName: String,
  zkHost: String,
  brokerList: String,
  consumerGroup: String,
  topic: String,
  partitions: Int,
  recordCount: Long,
  parallelism: Int,
  pattern: String,
  fieldIndex: Int,
  separator: String,
  prob: Double
)

object GearpumpConfig {
  final val BENCHCONFIG = "gearpump.bench.config"
}
