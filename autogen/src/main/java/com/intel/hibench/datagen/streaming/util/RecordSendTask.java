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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

public class RecordSendTask extends TimerTask {
  KafkaSender sender;
  private String topic;
  private boolean debugMode;

  private long recordsPerInterval; // Define how many records will be sent on each round
  private int totalRounds;         // Total times this task plan to run. -1 means run infinity
  private long totalRecords;       // Total records this task plan to sent. -1 means no limit.

  // Count how many rounds has run pre producer
  private int roundCounter = 0;

  // Count how many records has sent globally
  private static AtomicLong recordsCounter = new AtomicLong(0L);
  private Timer timer;     // The timer which executor this task

  private Boolean stopped = false;

  // Constructors
  public RecordSendTask(KafkaSender sender, String topic,
      long recordsPerInterval, int totalRounds, long totalRecords, boolean debugMode, Timer timer) {

    this.sender = sender;
    this.topic = topic;
    this.recordsPerInterval = recordsPerInterval;
    this.totalRounds = totalRounds;
    this.totalRecords = totalRecords;
    this.debugMode = debugMode;
    this.timer = timer;
    System.out.println(Thread.currentThread().getName() + " - starting generate data ... ");
  }

  @Override
  public void run() {
    synchronized(this) {
      if (debugMode) {
        String threadName = Thread.currentThread().getName();
        System.out.println( threadName + " - RecordSendTask run, " +
            roundCounter + " round, " + recordsCounter + " records sent");
      }

      if (isRecordValid() && isRoundValid()) {
        // Send records to Kafka
        long sentRecords = sender.send(topic, recordsPerInterval, debugMode);

        // Update counter
        roundCounter++;
        recordsCounter.getAndAdd(sentRecords);
      } else {
        if(!stopped) {
          sender.close();
          timer.cancel();
          timer.purge();
          stopped = true;
          // exit timer thread
          System.out.println("DataGenerator stop, " +
              roundCounter + " round, " + recordsCounter + " records sent");
        }
      }
    }
  }

  // Check round times, if it's bigger than total rounds, terminate data generator
  private boolean isRoundValid() {
    return (-1 == totalRounds) || (roundCounter < totalRounds);
  }

  // Check sent record number, if it's bigger than total records, terminate data generator
  private boolean isRecordValid() {
    return (-1 == totalRecords) || (recordsCounter.get() < totalRecords);
  }
}
