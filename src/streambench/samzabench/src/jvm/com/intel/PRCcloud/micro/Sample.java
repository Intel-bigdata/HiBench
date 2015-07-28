package com.intel.PRCcloud.micro;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class Sample implements StreamTask {
  private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "sample");

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String message = (String) envelope.getMessage();
    double randVal = Math.random();
    if (randVal <= CommonArg.getProb()) {
      collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, message));
    }
  }
}
