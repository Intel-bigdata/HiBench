package com.intel.hibench.streambench.storm.util;

import com.intel.hibench.streambench.common.metrics.LatencyReporter;

import java.io.Serializable;

public class StormBenchConfig {
  public String zkHost;
  public int workerCount;
  public int spoutThreads;
  public int boltThreads;
  public String benchName;
  public String topic;
  public String consumerGroup;
  public boolean readFromStart;
  public boolean ackon;
  public double prob;
  public LatencyReporter latencyReporter;

  //Following are fields that are benchmark specific
  public String separator = "\\s+";
  public int fieldIndex = 1;
  public String pattern = " ";
}