package com.intel.PRCcloud.micro;

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
import org.apache.samza.storage.kv.KeyValueStore;

public class Wordcount implements StreamTask, InitableTask {
  private KeyValueStore<String, Integer> store;

  public void init(Config config, TaskContext context) {
    this.store = (KeyValueStore<String, Integer>) context.getStore("my-store");
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String message = (String) envelope.getMessage();
    String separator = CommonArg.getSeparator();
    String[] fields = message.split(separator);
    for (String word:fields) {
        Integer count = store.get(word);
        if (count == null) count = 0;
        count++;
        store.put(word, count);
    }
  }
}
