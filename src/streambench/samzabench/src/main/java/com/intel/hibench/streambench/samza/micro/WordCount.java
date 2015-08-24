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
