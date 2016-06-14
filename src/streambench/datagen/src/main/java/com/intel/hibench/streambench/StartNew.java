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

import java.io.BufferedReader;

/**
 * @deprecated replace by DataGenerator
 */
@Deprecated
//Data generators are deployed in different nodes and run by launching them near simultaneously in different nodes.
public class StartNew {

  private static String benchName;
  private static String HDFSMaster;

  private static String userVisitsFile;
  private static long userVisitsFileOffset;

  private static String kMeansFile;
  private static long kMeansFileOffset;

  public static void main(String[] args) {
    if (args.length < 5) {
      System.err.println("args: <ConfigFile> <userVisitsFile> <userVisitsFileOffset> <kMeansFile> <kMeansFileOffset> need to be specified!");
      System.exit(1);
    }

    ConfigLoader configLoader = new ConfigLoader(args[0]);

    benchName = configLoader.getProperty("hibench.streamingbench.benchname").toLowerCase();
    HDFSMaster = configLoader.getProperty("hibench.hdfs.master");

    String topic = configLoader.getProperty("hibench.streamingbench.topic_name");
    String brokerList = configLoader.getProperty("hibench.streamingbench.brokerList");

    long totalCount = Long.parseLong(configLoader.getProperty("hibench.streamingbench.prepare.push.records"));
    userVisitsFile = args[1];
    userVisitsFileOffset = Long.parseLong(args[2]);
    userVisitsFile = args[1];
    userVisitsFileOffset = Long.parseLong(args[2]);
    boolean isNumericData = false;
    if (benchName.contains("statistics")) {
      isNumericData = true;
    }

    KafkaConnector con = new KafkaConnector(brokerList, configLoader);

    long recordsSent = 0L;
     while (recordsSent < totalCount) {
      recordsSent += con.sendRecords(getReader(), topic, totalCount - recordsSent, isNumericData);
    }

    con.close();
  }

  public static BufferedReader getReader() {
    FileDataGenNew files = new FileDataGenNew(HDFSMaster);
    if (benchName.contains("statistics")) {
      return files.loadDataFromFile(kMeansFile, kMeansFileOffset);
    } else {
      return files.loadDataFromFile(userVisitsFile, userVisitsFileOffset);
    }
  }
}
