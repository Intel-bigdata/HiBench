package com.intel.hibench.sparkbench.streaming.util

import org.apache.spark.storage.StorageLevel

case class SparkBenchConfig (
  // Spark
  master: String,
  benchName: String,
  batchInterval: Int,
  receiverNumber: Int,
  copies: Int,
  enableWAL: Boolean,
  checkpointPath: String,
  directMode: Boolean,

  // Kafka
  zkHost: String,
  consumerGroup: String,
  sourceTopic: String,
  reporterTopic: String,
  brokerList: String,


  // Hibench
  debugMode: Boolean,
  coreNumber: Int,
  sampleProbability: Double,
  windowDuration: Long,
  windowSlideStep: Long) {

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
