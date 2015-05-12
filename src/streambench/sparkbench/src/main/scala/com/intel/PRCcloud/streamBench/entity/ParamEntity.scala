package com.intel.PRCcloud.streamBench.entity

/**
 * Created by luruirui on 14-11-26.
 */
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
  brokerList: String
)
