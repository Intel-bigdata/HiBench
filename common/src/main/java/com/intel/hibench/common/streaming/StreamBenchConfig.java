/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.common.streaming;

/**
 * All name of configurations used in StreamBench are defined here. Later I plan to refactor
 * property name. With this mapping layer, the underlying Java/Scala code don't need to be
 * changed.
 */
public class StreamBenchConfig {
  // =====================================
  // General StreamBench Conf
  // =====================================
  public static String TESTCASE = "hibench.streambench.testCase";

  public static String ZK_HOST = "hibench.streambench.zkHost";

  public static String CONSUMER_GROUP = "hibench.streambench.kafka.consumerGroup";

  public static String KAFKA_TOPIC = "hibench.streambench.kafka.topic";

  public static String KAFKA_BROKER_LIST = "hibench.streambench.kafka.brokerList";

  public static String KAFKA_OFFSET_RESET = "hibench.streambench.kafka.offsetReset";

  public static String KAFKA_TOPIC_PARTITIONS = "hibench.streambench.kafka.topicPartitions";

  public static String DEBUG_MODE = "hibench.streambench.debugMode";

  // =====================================
  // TestCase related
  // =====================================
  // TODO: Once we remove all sample testcases, this config could be removed.
  public static String SAMPLE_PROBABILITY = "hibench.streambench.sampleProbability";

  public static String FixWINDOW_DURATION = "hibench.streambench.fixWindowDuration";

  public static String FixWINDOW_SLIDESTEP = "hibench.streambench.fixWindowSlideStep";

  // =====================================
  // Data Generator Related Conf
  // =====================================
  public static String DATAGEN_RECORDS_PRE_INTERVAL = "hibench.streambench.datagen.recordsPerInterval";

  public static String DATAGEN_INTERVAL_SPAN = "hibench.streambench.datagen.intervalSpan";

  public static String DATAGEN_TOTAL_RECORDS = "hibench.streambench.datagen.totalRecords";

  public static String DATAGEN_TOTAL_ROUNDS = "hibench.streambench.datagen.totalRounds";

  public static String DATAGEN_RECORD_LENGTH = "hibench.streambench.datagen.recordLength";

  public static String DATAGEN_PRODUCER_NUMBER = "hibench.streambench.datagen.producerNumber";
  // =====================================
  // Spark Streaming Related Conf
  // =====================================
  public static String SPARK_BATCH_INTERVAL = "hibench.streambench.spark.batchInterval";

  public static String SPARK_CHECKPOINT_PATH = "hibench.streambench.spark.checkpointPath";

  public static String SPARK_ENABLE_WAL = "hibench.streambench.spark.enableWAL";

  public static String SPARK_USE_DIRECT_MODE = "hibench.streambench.spark.useDirectMode";

  public static String SPARK_STORAGE_LEVEL = "hibench.streambench.spark.storageLevel";

  public static String SPARK_RECEIVER_NUMBER = "hibench.streambench.spark.receiverNumber";

  // ======================================
  // Flink Related Conf
  // ======================================
  public static String FLINK_BUFFERTIMEOUT = "hibench.streambench.flink.bufferTimeout";

  public static String FLINK_CHECKPOINTDURATION = "hibench.streambench.flink.checkpointDuration";

  // ======================================
  // Storm Related Conf
  // ======================================
  public static String STORM_WORKERCOUNT = "hibench.streambench.storm.worker_count";
  public static String STORM_SPOUT_THREADS = "hibench.streambench.storm.spout_threads";
  public static String STORM_BOLT_THREADS = "hibench.streambench.storm.bolt_threads";
  public static String STORM_ACKON = "hibench.streambench.storm.ackon";
  public static String STORM_LOCAL_SHUFFLE = "hibench.streambench.storm.localshuffle";

  // =====================================
  // Gearpump Related Conf
  // =====================================
  public static String GEARPUMP_PARALLELISM = "hibench.streambench.gearpump.parallelism";

}
