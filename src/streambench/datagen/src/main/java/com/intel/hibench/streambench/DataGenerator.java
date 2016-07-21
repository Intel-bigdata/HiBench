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

import com.intel.hibench.streambench.common.ConfigLoader;

import java.util.Timer;

public class DataGenerator {
  private static String benchName;

  private static String userVisitsFile;
  private static long userVisitsFileOffset;

  private static String kMeansFile;
  private static long kMeansFileOffset;

  public static void main(String[] args) {
    if (args.length < 5) {
      System.err.println("args: <ConfigFile> <userVisitsFile> <userVisitsFileOffset> <kMeansFile> <kMeansFileOffset> need to be specified!");
      System.exit(1);
    }

    // initialize variable from configuration and input parameters.
    ConfigLoader configLoader = new ConfigLoader(args[0]);

    benchName = configLoader.getProperty("hibench.streamingbench.benchname").toLowerCase();

    userVisitsFile = args[1];
    userVisitsFileOffset = Long.parseLong(args[2]);
    kMeansFile = args[3];
    kMeansFileOffset = Long.parseLong(args[4]);

    String topic = configLoader.getProperty("hibench.streamingbench.topic_name");
    long recordPerInterval = Long.parseLong(configLoader.getProperty("hibench.streamingbench.prepare.periodic.recordPerInterval"));
    long totalCount = Long.parseLong(configLoader.getProperty("hibench.streamingbench.prepare.push.records"));
    int intervalSpan = Integer.parseInt(configLoader.getProperty("hibench.streamingbench.prepare.periodic.intervalSpan"));
    int totalRound = Integer.parseInt(configLoader.getProperty("hibench.streamingbench.prepare.periodic.totalRound"));

    // instantiate KafkaSender
    KafkaSender sender;
    if(benchName.contains("statistics")) {
      sender = new KafkaSender(kMeansFile, kMeansFileOffset, configLoader, totalCount);
    } else {
      sender = new KafkaSender(userVisitsFile, userVisitsFileOffset, configLoader, totalCount);
    }

    Timer timer = new Timer();
    timer.schedule(new RecordSendTask(sender, topic, recordPerInterval, totalRound, totalCount), 0, intervalSpan);

    System.out.println("Record Per Round: " + recordPerInterval);
    System.out.println("TotalRecord: " + totalCount);
    System.out.println("TotalRound: " + totalRound);
    System.out.println("Timer scheduled, interval is " + intervalSpan + " ms");
  }
}
