package com.intel.hibench.streambench.samza.micro;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class Projection implements StreamTask {
  private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "projection");

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String message = (String) envelope.getMessage();
    String separator = CommonArg.getSeparator();
    String[] fields = message.split(separator);
    int fieldIndex = CommonArg.getFieldIndex();
    if (fields.length > fieldIndex) {
      collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, fields[fieldIndex]));
    }
  }
}
