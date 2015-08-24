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
