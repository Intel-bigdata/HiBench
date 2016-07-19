package com.intel.hibench.streambench.storm.util;

import com.intel.hibench.streambench.common.metrics.LatencyReporter;

import java.io.Serializable;

public class StormBenchConfig {
  public String nimbus;
  public int nimbusAPIPort;
  public String zkHost;
  public int workerCount;
  public int spoutThreads;
  public int boltThreads;
  public String benchName;
  public long recordCount;
  public String topic;
  public String consumerGroup;
  public boolean readFromStart;
  public boolean ackon;
  public int nimbusContactInterval;
  public LatencyReporter latencyReporter;

  //Following are fields that are benchmark specific
  public String separator;
  public int fieldIndex;
  public double prob;
  public String pattern;
}