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

package com.intel.hibench.flinkbench;

import com.intel.hibench.flinkbench.microbench.*;
import com.intel.hibench.flinkbench.util.BenchLogUtil;
import com.intel.hibench.flinkbench.util.FlinkBenchConfig;
import com.intel.hibench.common.streaming.ConfigLoader;
import com.intel.hibench.common.streaming.metrics.MetricsUtil;
import com.intel.hibench.common.streaming.StreamBenchConfig;
import com.intel.hibench.common.streaming.Platform;

public class RunBench {
  public static void main(String[] args) throws Exception {
    runAll(args);
  }

  public static void runAll(String[] args) throws Exception {

    if (args.length < 1)
      BenchLogUtil.handleError("Usage: RunBench <ConfigFile>");

    ConfigLoader cl = new ConfigLoader(args[0]);

    // Prepare configuration
    FlinkBenchConfig conf = new FlinkBenchConfig();
    conf.brokerList = cl.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST);
    conf.zkHost = cl.getProperty(StreamBenchConfig.ZK_HOST);
    conf.testCase = cl.getProperty(StreamBenchConfig.TESTCASE);
    conf.topic = cl.getProperty(StreamBenchConfig.KAFKA_TOPIC);
    conf.consumerGroup = cl.getProperty(StreamBenchConfig.CONSUMER_GROUP);
    conf.bufferTimeout = Long.parseLong(cl.getProperty(StreamBenchConfig.FLINK_BUFFERTIMEOUT));
    conf.offsetReset = cl.getProperty(StreamBenchConfig.KAFKA_OFFSET_RESET);
    conf.windowDuration = cl.getProperty(StreamBenchConfig.FixWINDOW_DURATION);
    conf.windowSlideStep = cl.getProperty(StreamBenchConfig.FixWINDOW_SLIDESTEP);

    conf.checkpointDuration = Long.parseLong(cl.getProperty(StreamBenchConfig.FLINK_CHECKPOINTDURATION));
    int producerNum = Integer.parseInt(cl.getProperty(StreamBenchConfig.DATAGEN_PRODUCER_NUMBER));
    long recordsPerInterval = Long.parseLong(cl.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL));
    int intervalSpan = Integer.parseInt(cl.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN));
    conf.reportTopic = MetricsUtil.getTopic(Platform.FLINK, conf.testCase, producerNum, recordsPerInterval, intervalSpan);
    int reportTopicPartitions = Integer.parseInt(cl.getProperty(StreamBenchConfig.KAFKA_TOPIC_PARTITIONS));
    MetricsUtil.createTopic(conf.zkHost, conf.reportTopic, reportTopicPartitions);

    // Main testcase logic
    String testCase = conf.testCase;

    if (testCase.equals("wordcount")) {
      WordCount wordCount = new WordCount();
      wordCount.processStream(conf);
    } else if (testCase.equals("identity")) {
      Identity identity = new Identity();
      identity.processStream(conf);
    } else if (testCase.equals("repartition")) {
      Repartition repartition = new Repartition();
      repartition.processStream(conf);
    } else if (testCase.equals("fixwindow")) {
      FixedWindow window = new FixedWindow();
      window.processStream(conf);
    }
  }
}
