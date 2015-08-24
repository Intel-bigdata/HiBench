package com.intel.hibench.streambench.samza.micro;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class ExtractForDistinctcount implements StreamTask {
  // Send outgoing messages to a stream called "words" in the "kafka" system.
  private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "words");

  public void process(IncomingMessageEnvelope envelope,
                      MessageCollector collector,
                      TaskCoordinator coordinator) {
    String message = (String) envelope.getMessage();
    String separator = CommonArg.getSeparator();
    String[] fields = message.split(separator);
    int fieldIndex = CommonArg.getFieldIndex();
    if (fields.length > fieldIndex) {
      String val = fields[fieldIndex];
      collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, val, val));
    }
  }
}
