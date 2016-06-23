package com.intel.hibench.streambench.spark.util

import org.apache.spark.storage.StorageLevel

case class SparkBenchConfig (
  // Spark
  master: String,
  appName: String,
  batchInterval: Int,
  receiverNumber: Int,
  copies: Int,
  enableWAL: Boolean,
  checkpointPath: String,
  directMode: Boolean,

  // Kafka
  zkHost: String,
  consumerGroup: String,
  topic: String,
  brokerList: String,


  // Hibench
  recordCount: Long,   // it's used in listener to terminate the application
  debugMode: Boolean,
  coreNumber: Int) {


  def storageLevel = copies match {
    case 0 => StorageLevel.MEMORY_ONLY
    case 1 => StorageLevel.MEMORY_AND_DISK_SER
    case _ => StorageLevel.MEMORY_AND_DISK_SER_2
  }

  def kafkaParams = Map (
    "group.id" -> consumerGroup,
    "zookeeper.connect" -> zkHost,
    "metadata.broker.list" -> brokerList
  )

  def threadsPerReceiver = coreNumber / receiverNumber
}
