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

package com.intel.hibench.streambench;

import com.intel.hibench.common.HiBenchConfig;
import com.intel.hibench.streambench.common.ConfigLoader;
import com.intel.hibench.streambench.common.StreamBenchConfig;
import com.intel.hibench.streambench.util.KafkaSender;
import com.intel.hibench.streambench.util.RecordSendTask;

import java.util.Timer;

public class DataGenerator {

  public static void main(String[] args) {
    if (args.length < 5) {
      System.err.println("args: <ConfigFile> <userVisitsFile> <userVisitsFileOffset> <kMeansFile> <kMeansFileOffset> need to be specified!");
      System.exit(1);
    }

    // initialize variable from configuration and input parameters.
    ConfigLoader configLoader = new ConfigLoader(args[0]);

    String userVisitsFile = args[1];
    long userVisitsFileOffset = Long.parseLong(args[2]);
    String kMeansFile = args[3];
    long kMeansFileOffset = Long.parseLong(args[4]);

    // load properties from config file
    String testCase = configLoader.getProperty(StreamBenchConfig.TESTCASE).toLowerCase();
    String topic = configLoader.getProperty(StreamBenchConfig.KAFKA_TOPIC);
    String brokerList = configLoader.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST);
    int intervalSpan = Integer.parseInt(configLoader.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN));
    long recordsPerInterval = Long.parseLong(configLoader.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL));
    long totalRecords = Long.parseLong(configLoader.getProperty(StreamBenchConfig.DATAGEN_TOTAL_RECORDS));
    int totalRounds = Integer.parseInt(configLoader.getProperty(StreamBenchConfig.DATAGEN_TOTAL_ROUNDS));
    int recordLength = Integer.parseInt(configLoader.getProperty(StreamBenchConfig.DATAGEN_RECORD_LENGTH));
    String dfsMaster = configLoader.getProperty(HiBenchConfig.DFS_MASTER);


    // instantiate KafkaSender
    KafkaSender sender;
    if(testCase.contains("statistics")) {
      sender = new KafkaSender(brokerList, kMeansFile, kMeansFileOffset, dfsMaster, recordLength, intervalSpan);
    } else {
      sender = new KafkaSender(brokerList, userVisitsFile, userVisitsFileOffset, dfsMaster, recordLength, intervalSpan);
    }

    // Schedule timer task
    Timer timer = new Timer();
    timer.schedule(
      new RecordSendTask(sender, topic, recordsPerInterval, totalRounds, totalRecords),
      0, intervalSpan);

    // Print out some basic information
    System.out.println("============ StreamBench Data Generator ============");
    System.out.println(" Interval Span       : " + intervalSpan + " ms");
    System.out.println(" Record Per Interval : " + recordsPerInterval);
    System.out.println(" Record Length       : " + recordLength + " bytes");

    if(totalRecords == -1) {
      System.out.println(" Total Records        : -1 [Infinity]");
    } else {
      System.out.println(" Total Records        : " + totalRecords);
    }

    if (totalRounds == -1) {
      System.out.println(" Total Rounds         : -1 [Infinity]");
    } else {
      System.out.println(" Total Rounds         : " + totalRounds);
    }

    System.out.println("====================================================");
  }
}
