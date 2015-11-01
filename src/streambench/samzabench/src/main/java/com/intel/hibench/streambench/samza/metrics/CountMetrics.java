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

package com.intel.hibench.streambench.samza.metrics;

import java.util.Map;
import java.util.HashMap;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.apache.samza.metrics.reporter.MetricsSnapshot;

public class CountMetrics implements StreamTask {
  static int sum = 0;
  static HashMap<String, Integer> smap = new HashMap<String, Integer>();

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    MetricsSnapshot message = (MetricsSnapshot) envelope.getMessage();
    HashMap<String, Object> imap = (HashMap<String, Object>) message.getAsMap();
    HashMap<String, Object> header = (HashMap<String, Object>) imap.get("header");
    String containerName = (String) header.get("container-name");
    if (containerName.equals("ApplicationMaster")) return;

    Map<String, Object> metrics = (Map<String, Object>) imap.get("metrics");
    if (!metrics.containsKey("org.apache.samza.container.TaskInstanceMetrics")) return;
    Map<String, Object> main = (Map<String, Object>) metrics.get("org.apache.samza.container.TaskInstanceMetrics");
    int num = (Integer) main.get("process-calls");

    if (smap.containsKey(containerName))
      sum -= smap.get(containerName);
    sum += num;
    smap.put(containerName, num);
    System.out.print(containerName + ": " + num);
    System.out.println("   Sum: " + sum);
  }
}
