package com.intel.PRCcloud.micro;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class Extract implements StreamTask {
  // Send outgoing messages to a stream called "numbers" in the "kafka" system.
  // Need this topic with only one partition, due to a global result!
  private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "numbers");

  public void process(IncomingMessageEnvelope envelope,
                      MessageCollector collector,
                      TaskCoordinator coordinator) {
    String message = (String) envelope.getMessage();
    String separator = CommonArg.getSeparator();
    String[] fields = message.split(separator);
    int fieldIndex = CommonArg.getFieldIndex();
    if (fields.length > fieldIndex) {
      String val = fields[fieldIndex];
      collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, val));
    }
  }
}
