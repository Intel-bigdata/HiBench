package com.intel.PRCcloud.micro;

import java.util.HashSet;
import java.util.Set;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskContext;
import org.apache.samza.config.Config;

public class Statistics implements StreamTask {
  private Long max = 0L;
  private Long min = 10000L;
  private Long sum = 0L;
  private Long count = 0L;

  public void reduce(Long v) {
    if (v > max) max = v;
    if (v < min) min = v;
    sum += v;
    count += 1;
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String message = (String) envelope.getMessage();
    Long val = Long.parseLong(message);
    reduce(val);
    System.out.println("max: " + max + 
                       " min: " + min +
                       " sum: " + sum +
                       " count: " + count);
  }
}
