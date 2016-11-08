package com.intel.hibench.streambench.spark.entity

case class ParamEntity(
  master: String,
  appName: String,
  batchInterval: Int,
  zkHost: String, 
  consumerGroup: String,
  topic: String,
  threads: Int,
  recordCount: Long,
  copies: Int,
  testWAL: Boolean,
  path: String,
  debug: Boolean,
  directMode: Boolean,
  brokerList: String,
  totalParallel: Int
)
