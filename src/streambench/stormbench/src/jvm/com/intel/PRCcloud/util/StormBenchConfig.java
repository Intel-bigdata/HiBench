package com.intel.PRCcloud.util;

public class StormBenchConfig{
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
  
  //Following are fields that are benchmark specific
  public String separator;
  public int fieldIndex;
  public double prob;
  public String pattern;
}