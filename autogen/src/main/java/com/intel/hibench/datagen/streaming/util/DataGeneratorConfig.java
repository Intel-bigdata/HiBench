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

package com.intel.hibench.datagen.streaming.util;

// A POJO class to contain necessary configuration
public class DataGeneratorConfig {
  String testCase;
  String brokerList;
  String kMeansFile;
  long kMeansFileOffset;
  String userVisitsFile;
  long userVisitsFileOffset;
  String dfsMaster;
  int recordLength;
  int intervalSpan;
  String topic;
  long recordsPerInterval;
  int totalRounds;
  long totalRecords;
  boolean debugMode;

  public DataGeneratorConfig(String testCase, String brokerList, String kMeansFile, long kMeansFileOffset,
      String userVisitsFile, long userVisitsFileOffset, String dfsMaster, int recordLength, int intervalSpan,
      String topic, long recordsPerInterval, int totalRounds, long totalRecords, boolean debugMode) {
    this.testCase = testCase;
    this.brokerList = brokerList;
    this.kMeansFile = kMeansFile;
    this.kMeansFileOffset = kMeansFileOffset;
    this.userVisitsFile = userVisitsFile;
    this.userVisitsFileOffset = userVisitsFileOffset;
    this.dfsMaster = dfsMaster;
    this.recordLength = recordLength;
    this.intervalSpan = intervalSpan;
    this.topic = topic;
    this.recordsPerInterval = recordsPerInterval;
    this.totalRounds = totalRounds;
    this.totalRecords = totalRecords;
    this.debugMode = debugMode;
  }

  public String getTestCase() {
    return testCase;
  }

  public String getBrokerList() {
    return brokerList;
  }

  public String getkMeansFile() {
    return kMeansFile;
  }

  public long getkMeansFileOffset() {
    return kMeansFileOffset;
  }

  public String getUserVisitsFile() {
    return userVisitsFile;
  }

  public long getUserVisitsFileOffset() {
    return userVisitsFileOffset;
  }

  public String getDfsMaster() {
    return dfsMaster;
  }

  public int getRecordLength() {
    return recordLength;
  }

  public int getIntervalSpan() { return intervalSpan; }

  public String getTopic() {
    return topic;
  }

  public long getRecordsPerInterval() {
    return recordsPerInterval;
  }

  public int getTotalRounds() {
    return totalRounds;
  }

  public long getTotalRecords() {
    return totalRecords;
  }

  public boolean getDebugMode() { return debugMode; }
}
