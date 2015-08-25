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

package com.intel.hibench.streambench.samza.micro;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskContext;
import org.apache.samza.config.Config;

public class WordCount implements StreamTask {
  Map<String, Integer> store = new HashMap<String, Integer>();

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String word = (String) envelope.getMessage();
    Integer count = store.get(word);
    if (count == null) count = 0;
    count++;
    store.put(word, count);
    System.out.println("Word: " + word + " / Count: " + count);
  }
}
