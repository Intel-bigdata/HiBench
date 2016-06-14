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

import java.util.TimerTask;

class RecordSendTask extends TimerTask {
  KafkaSender sender;
  private String topic;

  private long recordsPerRound;   // define how many reocrds will be sent on each round
  private int totalRounds;        // total times this task plan to run. -1 means run infinity
  private int roundCounter = 0;   // count how many rounds has run
  private long totalRecords;      // total records this task plan to sent. -1 means no limit.
  private long recordCounter = 0L;// count how many records has sent

  // Constructors
  public RecordSendTask(KafkaSender sender, String topic, long recordsPerRound, int totalRounds, long totalRecords) {
    this.sender = sender;
    this.topic = topic;
    this.recordsPerRound = recordsPerRound;
    this.totalRounds = totalRounds;
    this.totalRecords = totalRecords;
  }

  public RecordSendTask(KafkaSender sender, String topic, long recordsPerRound) {
    this.sender = sender;
    this.topic = topic;
    this.recordsPerRound = recordsPerRound;
    this.totalRounds = -1;
    this.totalRecords = -1;
  }

  public RecordSendTask(KafkaSender sender, String topic, long recordsPerRound, int totalRounds) {
    this.sender = sender;
    this.topic = topic;
    this.recordsPerRound = recordsPerRound;
    this.totalRounds = totalRounds;
    this.totalRecords = -1;
  }

  @Override
  public void run() {
    System.out.println("RecordSendTask run, " + roundCounter + " round.");

    if (isRecordValid() && isRoundValid()) {
      // send records to Kafka
      long sentRecords = sender.send(topic, recordsPerRound);
      System.out.println("sent " + sentRecords + " records on record " + roundCounter);

      //update counter
      roundCounter++;
      recordCounter += sentRecords;
    } else {
      // exit application
      System.out.println("DataGenerator stop, run "
          + roundCounter + " round, sent " + recordCounter +" records.");
      sender.close();
      System.exit(0);
    }
  }

  // check round times, if it's bigger than total rounds, terminate data generator
  private boolean isRoundValid() {
    return (-1 == totalRounds) || (roundCounter < totalRounds);
  }

  // check sent record number, if it's bigger than total records, terminate data generator
  private boolean isRecordValid() {
    return (-1 == totalRecords) || (recordCounter < totalRecords);
  }
}
