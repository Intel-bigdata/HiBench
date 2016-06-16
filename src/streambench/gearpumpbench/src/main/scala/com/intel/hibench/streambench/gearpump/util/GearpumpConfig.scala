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

  val BENCH_CONFIG = "gearpump.bench.config"
  val BENCH_LATENCY_REPORTER = "gearpump.bench.latency.reporter"
}
