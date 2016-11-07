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

package com.intel.hibench.stormbench;

import com.intel.hibench.common.streaming.ConfigLoader;
import com.intel.hibench.common.streaming.Platform;
import com.intel.hibench.common.streaming.StreamBenchConfig;
import com.intel.hibench.common.streaming.TestCase;
import com.intel.hibench.common.streaming.metrics.MetricsUtil;
import com.intel.hibench.stormbench.micro.*;
import com.intel.hibench.stormbench.trident.*;
import com.intel.hibench.stormbench.util.BenchLogUtil;
import com.intel.hibench.stormbench.util.StormBenchConfig;

public class RunBench {

  public static void main(String[] args) throws Exception {
    runAll(args);
  }

  private static void runAll(String[] args) throws Exception {

    if (args.length < 2)
      BenchLogUtil.handleError("Usage: RunBench <ConfigFile> <FrameworkName>");

    StormBenchConfig conf = new StormBenchConfig();

    ConfigLoader cl = new ConfigLoader(args[0]);
    boolean TridentFramework = false;
    if (args[1].equals("trident")) TridentFramework = true;

    conf.zkHost = cl.getProperty(StreamBenchConfig.ZK_HOST);
    conf.workerCount = Integer.parseInt(cl.getProperty(StreamBenchConfig.STORM_WORKERCOUNT));
    conf.spoutThreads = Integer.parseInt(cl.getProperty(StreamBenchConfig.STORM_SPOUT_THREADS));
    conf.boltThreads = Integer.parseInt(cl.getProperty(StreamBenchConfig.STORM_BOLT_THREADS));
    conf.benchName = cl.getProperty(StreamBenchConfig.TESTCASE);
    conf.topic = cl.getProperty(StreamBenchConfig.KAFKA_TOPIC);
    conf.consumerGroup = cl.getProperty(StreamBenchConfig.CONSUMER_GROUP);
    conf.ackon = Boolean.parseBoolean(cl.getProperty(StreamBenchConfig.STORM_ACKON));
    conf.localShuffle = Boolean.parseBoolean(cl.getProperty(StreamBenchConfig.STORM_LOCAL_SHUFFLE));

    conf.windowDuration = Long.parseLong(cl.getProperty(StreamBenchConfig.FixWINDOW_DURATION));
    conf.windowSlideStep = Long.parseLong(cl.getProperty(StreamBenchConfig.FixWINDOW_SLIDESTEP));

    conf.brokerList = cl.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST);
    int producerNum = Integer.parseInt(cl.getProperty(StreamBenchConfig.DATAGEN_PRODUCER_NUMBER));
    long recordPerInterval = Long.parseLong(cl.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL));
    int intervalSpan = Integer.parseInt(cl.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN));
    if (TridentFramework) {
      conf.reporterTopic = MetricsUtil.getTopic(Platform.TRIDENT,
          conf.topic, producerNum, recordPerInterval, intervalSpan);
    } else {
      conf.reporterTopic = MetricsUtil.getTopic(Platform.STORM,
          conf.topic, producerNum, recordPerInterval, intervalSpan);
    }
    int reportTopicPartitions = Integer.parseInt(cl.getProperty(StreamBenchConfig.KAFKA_TOPIC_PARTITIONS));
    MetricsUtil.createTopic(conf.zkHost, conf.reporterTopic, reportTopicPartitions);
    TestCase benchName = TestCase.withValue(conf.benchName);

    BenchLogUtil.logMsg("Benchmark starts... " + "  " + benchName +
        "   Frameworks:" + (TridentFramework ? "Trident" : "Storm"));

    if (TridentFramework) { // For trident workloads
      if (benchName.equals(TestCase.WORDCOUNT)) {
        TridentWordcount wordcount = new TridentWordcount(conf);
        wordcount.run();
      } else if (benchName.equals(TestCase.IDENTITY)) {
        TridentIdentity identity = new TridentIdentity(conf);
        identity.run();
      } else if (benchName.equals(TestCase.REPARTITION)) {
        TridentRepartition repartition = new TridentRepartition(conf);
        repartition.run();
      } else if (benchName.equals(TestCase.FIXWINDOW)) {
        TridentWindow window = new TridentWindow(conf);
        window.run();
      }
    } else { // For storm workloads
      if (benchName.equals(TestCase.IDENTITY)) {
        Identity identity = new Identity(conf);
        identity.run();
      } else if (benchName.equals(TestCase.WORDCOUNT)) {
        WordCount wordCount = new WordCount(conf);
        wordCount.run();
      } else if (benchName.equals(TestCase.FIXWINDOW)) {
        WindowedCount window = new WindowedCount(conf);
        window.run();
      }
    }
  }
}
