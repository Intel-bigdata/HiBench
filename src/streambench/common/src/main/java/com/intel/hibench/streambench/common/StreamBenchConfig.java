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

package com.intel.hibench.streambench.common;

/**
 * All name of configurations used in StreamBench are defined here. Later I plan to refactor
 * property name. With this mapping layer, the underlying Java/Scala code don't need to be
 * changed.
 */
public class StreamBenchConfig {
  // =====================================
  // General StreamBench Conf
  // =====================================
  // TODO: rename to "hibench.streambench.testcase"
  public static String TESTCASE = "hibench.streamingbench.benchname";

  // TODO: rename to "hibench.streambench.topic"
  public static String TOPIC = "hibench.streamingbench.topic_name";

  // TODO: rename to "hibench.streambench.zkHost"
  public static String ZK_HOST = "hibench.streamingbench.zookeeper.host";

  // TODO: rename to "hibench.streambench.consumerGroup"
  public static String CONSUMER_GROUP = "hibench.streamingbench.consumer_group";

  // TODO: rename to "hibench.streambench.brokerList"
  public static String BROKER_LIST = "hibench.streamingbench.brokerList";

  // TODO: rename to "hibench.streambench.debugMode"
  public static String DEBUG_MODE = "hibench.streamingbench.debug";

  public static String SEPARATOR = "hibench.streamingbench.separator";

  // TODO: rename to "hibench.streambench.grep.pattern"
  public static String GREP_PATTERN = "hibench.streamingbench.pattern";

  // TODO: rename to "hibench.streambench.sample.probability"
  public static String SAMPLE_PROBABILITY = "hibench.streamingbench.prob";


  // =====================================
  // Prepare Related Conf
  // =====================================
  // TODO: rename prepare to dataGen
  public static String PREPARE_RECORD_PRE_INTERVAL = "hibench.streamingbench.recordPerInterval";

  public static String PREPARE_INTERVAL_SPAN = "hibench.streamingbench.prepare.periodic.intervalSpan";

  // =====================================
  // Spark Streaming Related Conf
  // =====================================
  // TODO: rename to "hibench.streambench.spark.batchInterval"
  public static String SPARK_BATCH_INTERVAL = "hibench.streamingbench.batch_interval";

  // TODO: rename to "hibench.streambench.spark.checkpointPath"
  public static String SPARK_CHECKPOINT_PATHL = "hibench.streamingbench.checkpoint_path";

  // TODO: rename to "hibench.streambench.spark.enableWAL"
  public static String SPARK_ENABLE_WAL = "hibench.streamingbench.testWAL";

  // TODO: rename to "hibench.streambench.spark.useDirectMode"
  public static String SPARK_USE_DIRECT_MODE = "hibench.streamingbench.direct_mode";

  // TODO: rename to "hibench.streambench.spark.receiverNumber"
  public static String SPARK_RECEIVER_NUMBER = "hibench.streamingbench.receiver_nodes";

  // TODO: rename to "hibench.streambench.spark.storageLevel"
  public static String SPARK_STORAGE_LEVEL = "hibench.streamingbench.copies";

}
