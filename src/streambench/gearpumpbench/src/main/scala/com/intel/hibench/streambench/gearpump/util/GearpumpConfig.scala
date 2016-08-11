package com.intel.hibench.streambench.gearpump.util

case class GearpumpConfig(
  benchName: String,
  zkHost: String,
  brokerList: String,
  consumerGroup: String,
  topic: String,
  parallelism: Int,
  prob: Double,
  reporterTopic: String,
  pattern: String = " ",
  fieldIndex: Int = 0,
  separator: String = "\\s+"
)

object GearpumpConfig {

  val BENCH_CONFIG = "gearpump.bench.config"
  val BENCH_LATENCY_REPORTER = "gearpump.bench.latency.reporter"
}
